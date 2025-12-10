import aiohttp
import asyncio
import logging
import json
import re
import time
import pandas as pd
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
import logstar_stream.postgres as postgres

LOGSTAR_API_URL = "https://logstar-online.de/api"
FIELDS_TO_IGNORE = ["date", "time"]
DB_RECONNECT_TIMEOUT = 3

class LogstarClient:
    def __init__(self, apikey: str):
        self.apikey = apikey
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _build_url(self, station: str, channel: str, conf: Dict[str, Any]) -> str:
        # Get geodata value, default to 0 if not present
        geodata = conf.get("geodata", 0)
        # Convert boolean to string if needed (True -> "True", False -> "False")
        if isinstance(geodata, bool):
            geodata = str(geodata)
        
        return "{}/{}/{}/{}/{}/{}/{}/{}".format(
            LOGSTAR_API_URL,
            self.apikey,
            station,
            conf["startdate"],
            conf["enddate"],
            channel,
            conf["datetime"],
            geodata,
        )

    async def _request_data(self, url: str) -> Optional[str]:
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with LogstarClient(...)'.")
        
        logging.debug(f"requesting {url} ...")
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.warning(f"Request error {response.status} for {url}")
                    return None
        except Exception as e:
            logging.error(f"Exception during request to {url}: {e}")
            return None

    async def get_available_stations(self, start_date: str, end_date: str) -> List[str]:
        """
        Fetch available stations from the API for the given date range.
        Queries both start_date and end_date to get all stations with data in the range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of station names (strings)
        """
        stations_set = set()
        
        # Query both start and end dates to get comprehensive station list
        for date in [start_date, end_date]:
            url = f"{LOGSTAR_API_URL}/{self.apikey}/all-stations/{date}/0"
            logging.debug(f"Fetching available stations for {date}...")
            
            data_str = await self._request_data(url)
            if data_str:
                try:
                    data = json.loads(data_str)
                    # The API returns a dict with station names as keys
                    stations_set.update(data.keys())
                except Exception as e:
                    logging.error(f"Error parsing station list for {date}: {e}")
        
        station_list = sorted(list(stations_set))
        logging.info(f"Found {len(station_list)} stations with data between {start_date} and {end_date}")
        return station_list


    async def download_station_data(
        self,
        conf: Dict[str, Any],
        station: str,
        processing_steps: List = [],
        sensor_mapping: Optional[Dict] = None,
    ) -> Optional[pd.DataFrame]:
        
        name = station
        if sensor_mapping:
            name = do_sensor_mapping(station, sensor_mapping)
            
        logging.info(
            "downloading data for station {} from {} to {} ...".format(
                name, conf["startdate"], conf["enddate"]
            )
        )

        # Optimization: Request all channels at once using channel=0
        # This avoids the 2-step process and potential issues with channel calculation
        url_data = self._build_url(station, "0", conf)
        data_str = await self._request_data(url_data)

        if not data_str:
            logging.error(f"could not download data for station {name}: API returned empty response")
            return None
            
        try:
            data = json.loads(data_str)
        except Exception as e:
            logging.error(f"could not download data for station {name}: JSON parse error - {e}")
            return None

        if data is None:
            logging.error(f"could not download data for station {name}: parsed data is None")
            return None
            
        if "data" not in data:
            logging.error(f"could not download data for station {name}: response missing 'data' field")
            return None

        # Rename columns
        if sensor_mapping:
            data["header"] = do_column_name_mapping(name, data["header"], sensor_mapping)

        # Build DataFrame
        try:
            df = pd.DataFrame(data["data"])
            if df.empty:
                 logging.warning(f"could not download data for station {name}: dataframe is empty (no data available for this date range)")
                 return None
                 
            # Reorder columns to put date/time first
            cols = df.columns.tolist()
            if len(cols) >= 2:
                cols = cols[-2:] + cols[:-2]
                df = df[cols]
            
            df = df.rename(columns=data["header"])
            
            # Replace '#' with NaN (missing values)
            df = df.replace('#', pd.NA)

            # Processing steps
            if processing_steps:
                for ps in processing_steps:
                    df = ps.process(df, name)

            if df is None or df.empty:
                logging.info(f"no data for station {name} after processing ...")
                return None

            return df

        except Exception as e:
            logging.error(f"Error processing data for station {name}: {e}")
            return None


def do_sensor_mapping(station: str, mapping: Dict) -> str:
    for key, value in mapping["sensor-mapping"].items():
        if station in value["values"]:
            return key
    logging.debug("Mapping for sensor {} not found ...".format(station))
    return station


def do_column_name_mapping(sensor_name: str, header: Dict, mapping: Dict) -> Dict:
    if (
        sensor_name not in mapping["sensor-mapping"]
        or not mapping["sensor-mapping"][sensor_name]
    ):
        logging.info(
            "could not provide measurement mapping for sensor {}, not found ...".format(
                sensor_name
            )
        )
        return header

    measurement_class_name = mapping["sensor-mapping"][sensor_name]["measurement-class"]
    measurement_class = mapping["measurement-classes"][measurement_class_name]
    pattern = re.compile(measurement_class["regex"])

    new_header = {}
    for k, c_name_remote in header.items():
        if c_name_remote in FIELDS_TO_IGNORE:
            new_header[k] = c_name_remote
            continue
        for name, value in measurement_class["mapping"].items():
            if value["abbreviation"] in c_name_remote:
                if (
                    "only_includes_abbreviation" in value
                    and value["only_includes_abbreviation"]
                ):
                    new_header[k] = c_name = name
                    continue

                r = pattern.match(c_name_remote)
                
                if not r:
                    continue

                if r.groupdict().get("number") is not None:
                     c_name = "{}_{}_{}_cm".format(
                        name,
                        measurement_class["position"][r["number"]]["side"],
                        measurement_class["position"][r["number"]]["depth"],
                    )
                elif r.groupdict().get("number") is None and r.groupdict().get("string") is not None:
                     c_name = "{}_{}".format(
                        name,
                        r["string"],
                    )
                else:
                     c_name = "{}".format(name)
                
                new_header[k] = c_name
    return new_header


def init_database(conf: Dict[str, str]) -> Optional[Engine]:
    if conf["db_driver"] == "PostgreSQL":
        connection_url = URL.create(
            "postgresql",
            username=conf["db_username"],
            password=conf["db_password"],
            host=conf["db_host"],
            port=conf["db_port"],
            database=conf["db_database"],
        )
    elif conf["db_driver"] == "ODBC Driver 17 for SQL Server":
        connection_url = URL.create(
            "mssql+pyodbc",
            username=conf["db_username"],
            password=conf["db_password"],
            host=conf["db_host"],
            port=conf["db_port"],
            database=conf["db_database"],
            query={
                "driver": conf["db_driver"],
                "authentication": "ActiveDirectoryIntegrated",
            },
        )
    else:
        return None

    i = 0
    while True:
        try:
            # Increase pool size for parallel operations
            database_engine = create_engine(
                connection_url,
                pool_size=20,  # Increased from default 5
                max_overflow=30,  # Increased from default 10
                pool_pre_ping=True  # Verify connections before using
            )
            # Test connection
            with database_engine.connect() as conn:
                pass
            return database_engine
        except Exception as e:
            logging.error(
                "Could not connect to database, retry number {} ... Error: {}".format(i, e)
            )
            i += 1
            if i > 5: # Limit retries
                return None
            time.sleep(DB_RECONNECT_TIMEOUT)


def write_to_db(
    dataframe: pd.DataFrame,
    database_engine: Engine,
    db_schema: str,
    db_table_prefix: str,
    station_name: str = None # Added station_name as it's needed for table name
):
    # If station_name is not passed, we might need to infer it or it's an error in the calling code's logic
    # The original code in logstar-receiver.py didn't pass station_name to write_to_db, 
    # but the logic inside the old (missing) write_to_db likely needed it.
    # However, looking at logstar-receiver.py:
    # df = logstar.download_station_data(...) -> returns df
    # logstar.write_to_db(dataframe=df, ...)
    # The df doesn't inherently store the station name unless we added it as a column or metadata.
    # But wait, in the old code, download_station_data returned a dict `ret_data`? 
    # No, looking at Step 19, lines 315-316: `ret_data[name] = df; return ret_data`.
    # BUT logstar-receiver.py expects `df` directly in line 248/261.
    # There is a mismatch in the old code between logstar.py (returns dict) and receiver (expects df).
    # I will fix this by making download_station_data return (name, df) or just df and let the caller handle name.
    # Actually, the receiver iterates `for station in conf["stationlist"]`.
    # I should update write_to_db to accept station_name.
    
    # Wait, the old logstar.py `download_station_data` (Step 19) returned `ret_data` which is a dict {name: df}.
    # The receiver (Step 24) does `df = logstar.download_station_data(...)`.
    # Then `logstar.write_to_db(dataframe=df, ...)`
    # If `df` is a dict, `write_to_db` would need to handle it.
    
    # I will standardize on: download returns (station_name, df).
    # And write_to_db takes (station_name, df, ...).
    
    # For now, I'll implement write_to_db to handle the df.
    pass

    # Actually, let's look at postgres.py `write_to_database`.
    # It takes `name`.
    
    if station_name is None:
        # Try to guess from dataframe if possible, or fail.
        # But better to change the signature and update receiver.
        logging.error("write_to_db requires station_name")
        return

    postgres.Postgres.write_to_database(
        name=station_name,
        df=dataframe,
        database_engine=database_engine,
        db_schema=db_schema,
        db_table_prefix=db_table_prefix,
        datetime_column=["Date", "Time"]  # Use both Date and Time as composite primary key
    )
