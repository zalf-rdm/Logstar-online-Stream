import requests
import re
import logging
import json
import os
import csv
from typing import List, Dict
import pandas as pd

import sqlalchemy as sq

# only works for PostgreSQL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect

"""
	API DOCs
	http://dokuwiki.weather-station-data.com/doku.php?id=:en:start
"""
LOGSTAR_API_URL = "https://logstar-online.de/api"


# PostgreSQL interaction
# ref: https://stackoverflow.com/questions/30337394/pandas-to-sql-fails-on-duplicate-primary-key
def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
    """
    Insert all records from data_iter into table. If a record already exists (as determined by the primary keys), do nothing.

    :param table: the sqlalchemy table to insert into
    :type table: sqlalchemy.sql.schema.Table
    :param conn: the sqlalchemy connection to use
    :type conn: sqlalchemy.engine.Connection
    :param keys: the keys to use for determining uniqueness
    :type keys: List[str]
    :param data_iter: the data to insert
    :type data_iter: iterator over dictionaries
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=["Datetime"])
    result = conn.execute(stmt)
    return result.rowcount


# ref: https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
def create_table(
    table_name,
    database_engine,
    frame,
    if_exists="fail",
    index=True,
    index_label=None,
    keys=None,
    schema=None,
    dtype=None,
    **kwargs,
):
    """
    Creates a SQL table using the provided DataFrame and SQLAlchemy engine.

    Args:
        frame (DataFrame): The pandas DataFrame to be converted into a SQL table.
        name (str): The name of the SQL table to be created.
        if_exists (str, optional): What to do if the table already exists. Options are 'fail', 'replace', or 'append'. Defaults to 'fail'.
        index (bool, optional): Whether to include the DataFrame's index as a column in the table. Defaults to True.
        index_label (str or sequence, optional): Column label for the index column(s). If None is given (default) and index is True, the index names are used.
        schema (str, optional): The schema to create the table under. Defaults to None.
        chunksize (int, optional): If not None, then rows will be written in batches of this size. Defaults to None.
        dtype (dict, optional): Specifying the datatype for columns. If None, the data type will be inferred. Defaults to None.
        **kwargs: Additional arguments to pass to the SQLTable constructor.

    Raises:
        ValueError: If the specified dtype is not a valid SQLAlchemy type.
    """
    if dtype is not None:
        from sqlalchemy.types import to_instance, TypeEngine

        # check provided types
        for col, my_type in dtype.items():
            if not isinstance(to_instance(my_type), TypeEngine):
                raise ValueError("The type of %s is not a SQLAlchemy " "type " % col)
    
    table = pd.io.sql.SQLTable(
        table_name,
        database_engine,
        frame=frame,
        index=index,
        if_exists=if_exists,
        index_label=index_label,
        schema=schema,
        keys=keys,
        dtype=dtype,
        **kwargs,
    )
    table.create()

def build_url(conf, station, channel=0):
    """build url to request from
    docs: https://logstar-online.de/api/{apiKey}/{Stationname}/{StartTag}/{EndTag}/{Channellist}/{DateTime}/{GeoData}
    """
    apikey = conf["apikey"]
    startdate = conf["startdate"]
    enddate = conf["enddate"]
    datetime = conf["datetime"]
    geodata = conf["geodata"]
    url = f"{LOGSTAR_API_URL}/{apikey}/{station}/{startdate}/{enddate}/{channel}/{datetime}/{geodata}"
    return url


def do_station_name_mapping(station, mapping):
    """
    get readable name for given station from mapping


    :param station: The station to map.
    :type station: Any
    :param mapping: The mapping to use.
    :type mapping: dict
    :return: The mapped key or the original station.
    :rtype: Any
    """
    for key, value in mapping["sensor-mapping"].items():
        if station in value["values"]:
            return key
    logging.debug("Mapping for sensor {} not found ...".format(station))
    return station


FIELDS_TO_IGNORE = ["date", "time", "Datetime"]

def __find_sensor_mapping__(sensor_name, mapping):
    for key, value in mapping["sensor-mapping"].items():
        if sensor_name in value["values"]:
          return key
    return False

def do_column_name_mapping(sensor_name, header, mapping):
    """
    Maps column names in the header based on a sensor name and a mapping dictionary.

    Args:
        sensor_name (str): The name of the sensor.
        header (dict): The header dictionary containing column names as keys and column names as values.
        mapping (dict): The mapping dictionary containing sensor mappings and measurement classes.

    Returns:
        dict: A new header dictionary with mapped column names.
    """
    mapping_name = __find_sensor_mapping__(sensor_name,mapping)
    if not mapping_name:
        logging.info(
            "could not provide measurement mapping for sensor {}, not found ...".format(
                sensor_name
            )
        )
        return header

    measurement_class_name = mapping["sensor-mapping"][mapping_name]["measurement-class"]
    measurement_class = mapping["measurement-classes"][measurement_class_name]
    if "regex" in measurement_class:
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

                    # c_name_remote can differ a lot, the design of this names is not properly choosen by UP GmbH
                    # worst case is weather data which supports 3 different pattern:

                    # case 1: "WS1_LT_3 - °C
                    if r["number"] is not None:
                        c_name = "{}_{}_{}_cm".format(
                            name,
                            measurement_class["position"][r["number"]]["side"],
                            measurement_class["position"][r["number"]]["depth"],
                        )
                    # case 2 "WS1_WG_x - m/s"
                    elif r["number"] is None and r["string"] is not None:
                        c_name = "{}_{}".format(
                            name,
                            r["string"],
                        )
                    # case 3 "WS1_WR - grad"
                    elif r["number"] is None and r["string"] is None:
                        c_name = "{}".format(
                            name,
                        )
                    new_header[k] = c_name
        return new_header
    return None


def request_data(url, timeout):
    """
    Request data from a specified URL.

    Args:
        url (str): The URL to request data from.

    Returns:
        str or None: The response text if the request is successful, or None if there is an error.

    """
    logging.debug("requesting {} ...".format(url))
    r = requests.get(url, timeout=timeout)

    if r.status_code == 200:
        return r.text
    else:
        logging.error("Request error {}".format(r.status_code))
        exit(1)


def download_data(conf, station, timeout=15):
    """
    Downloads data from a given station.

    Args:
        conf (dict): The configuration settings for downloading the data.
        station (str): The name of the station to download data from.

    Returns:
        dict: The downloaded data as a dictionary, or None if the download fails.
    """
    url = build_url(conf, station=station)

    try:
        request = request_data(url, timeout=timeout)
        return json.loads(request)
    except:
        logging.error(
            f"Error when downloading data for station {station} using url {url}...\n{request}"
        )
        exit(1)


def prepare_dataframe(data: Dict, datetime_column: str) -> pd.DataFrame:
    # build pandas df from data
    df = pd.DataFrame(data["data"])
    df = df.rename(columns=data["header"])

    # depending on LOGSTAR_DAYTIME="0"
    # making datetime occure in beginning
    if "Datetime" in df.columns.tolist():
        df.rename(columns={"Datetime": datetime_column}, inplace=True)
        cols = df.columns.tolist()

        # set datetime column astype datetime
        df[datetime_column] = pd.to_datetime(df[datetime_column], errors="raise")

        # force datetime column to be on first position
        cols.insert(0, cols.pop(cols.index(datetime_column)))
        df = df[cols]

        cols.remove(datetime_column)

    # may buggy
    elif "Date" in cols and "Time" in cols:
        # making date and time occure in beginning
        cols.insert(0, cols.pop(cols.index("Date")))
        cols.insert(0, cols.pop(cols.index("Time")))
        cols = df.columns.tolist()
        df = df[cols]

        # clean cols for dtype optimization
        cols.remove("Date")
        cols.remove("Time")

    # replace # to be Nan , hashes are comming from UP as no-value
    df.replace("#", pd.NA, inplace=True)
    # update col type to numeric after replacing # value
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="raise")
    return df


def write_to_database(
    name, df, database_engine, db_schema, db_table_prefix, datetime_column, **kwargs
):
    table_name = db_table_prefix + name

    if not inspect(database_engine).has_table(table_name=table_name, schema=db_schema):
        logging.info(
            f"creating database table {table_name} with primary key on {datetime_column} ..."
        )
        with database_engine.begin() as conn:
            pandas_sql = pd.io.sql.pandasSQL_builder(conn, schema=db_schema)
            # create table with constrains
            create_table(
                frame=df,
                table_name=table_name,
                database_engine=pandas_sql,
                index=None,
                schema=db_schema,
                index_label=None,
                keys=datetime_column,
            )
    
    else:
        contrains = inspect(database_engine).get_pk_constraint(
            table_name=table_name, schema=db_schema
        )
        if (
            not "constrained_columns" in contrains
            or datetime_column not in contrains["constrained_columns"]
        ):
            logging.warning(
                f"Table {table_name} has no primary key set on {datetime_column} column, this can result in duplicated data in table  ..."
            )
    with database_engine.begin() as conn:
        to_sql_arugments = {
            "name": table_name,
            "con": database_engine,
            "schema": db_schema,
            "if_exists": "append",
            "index": False,
            "chunksize": 1024,
            "method": insert_or_do_nothing_on_conflict
        }

        try:
            num_rows = len(df)
            logging.info(f"Attempting to insert {num_rows} rows into {table_name} ...")
            df.to_sql(**to_sql_arugments)
            logging.info(f"succesfully writing data ...")
        except Exception as E:
            logging.error(f"failed writing data: {str(E)[:200]}")  # Print first 200 chars of error
            exit(1)

def manage_dl_db(
    conf,
    database_engine=None,
    processing_steps: List = [],
    sensor_mapping=None,
    csv_folder=None,
    db_schema=None,
    db_table_prefix=None,
    datetime_column="Datetime",
    timeout=15,
    **kwargs,
):
    """
    main routine to download data and save it to database and|or csv

    :param conf
    :param database_engine
    :param processing_steps
    :param sensor_mapping
    :param csv_folder
    :param db_schema
    :param db_table_prefix
    """
    ret_data = {}
    for station in conf["stationlist"]:
        name = station

        # download data
        logging.info(
            "downloading data for station {} from {} to {} ...".format(
                name, conf["startdate"], conf["enddate"]
            )
        )
        data = download_data(conf, station, timeout)
        # no new data or something went wrong while downloading the data
        if data is None or "data" not in data:
            logging.error(f"could not download data for station {name}\n {data}")
            continue

        # rename table column names, or csv column names
        if sensor_mapping:
            mapping_return = do_column_name_mapping(
                name, data["header"], sensor_mapping
            )

            # update columns names if mapping_return is not None
            data["header"] = (
                mapping_return if mapping_return is not None else data["header"]
            )
            # rename station if sensor_mapping available
            name = do_station_name_mapping(station, sensor_mapping)

        # get downloaded data as dataframe
        df = prepare_dataframe(data, datetime_column)

        # give data to process
        if processing_steps is not None:
            [df := ps.process(df, name) for ps in processing_steps]

        # check if dataframe is not empty
        if df is None or df.empty:
            logging.warning(f"empty dataframe for station {name}")
            ret_data[name] = df
            continue

        # if database engine is set, write to database
        if database_engine:
            write_to_database(
                name, df, database_engine, db_schema, db_table_prefix, datetime_column
            )

        # write to file
        if csv_folder:
            filepath = os.path.join(csv_folder, name + ".csv")
            df.to_csv(
                filepath,
                sep=",",
                quotechar='"',
                header=True,
                mode="a",
                doublequote=False,
                quoting=csv.QUOTE_MINIMAL,
                index=False,
            )

        # add df to return data collection
        ret_data[name] = df
    return ret_data
