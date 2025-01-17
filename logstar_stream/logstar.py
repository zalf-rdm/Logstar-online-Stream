import requests
import re
import logging
import json
import os
import csv
from typing import List

import sqlalchemy as sq
# only works for PostgreSQL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.inspection import inspect
import pandas as pd

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
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing()
    conn.execute(on_duplicate_key_stmt)


# ref: https://stackoverflow.com/questions/30867390/python-pandas-to-sql-how-to-create-a-table-with-a-primary-key
def create_table(self, frame, name, if_exists='fail', index=True,
          index_label=None, schema=None, chunksize=None, dtype=None, **kwargs):
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
        for col, my_type in dtype.items():
            if not isinstance(to_instance(my_type), TypeEngine):
                raise ValueError('The type of %s is not a SQLAlchemy '
                                'type ' % col)
    table = pd.io.sql.SQLTable(name, self, frame=frame, index=index,
                    if_exists=if_exists, index_label=index_label,
                    schema=schema, dtype=dtype, **kwargs)
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


def do_sensor_mapping(station, mapping):
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


FIELDS_TO_IGNORE = ["date", "time"]
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
    try:
        r = requests.get(url, timeout=timeout)
    except:
        return None
    if r.status_code == 200:
        return r.text
    else:
        logging.debug("Request error {}".format(r.status_code))
        return None


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
        return None


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
        # rename station if sensor_mapping available
        if sensor_mapping:
            name = do_sensor_mapping(station, sensor_mapping)
        logging.info(
            "downloading data for station {} from {} to {} ...".format(
                name, conf["startdate"], conf["enddate"]
            )
        )
        # download data
        data = download_data(conf, station, timeout)

        # no new data or something went wrong while downloading the data
        if data is None or "data" not in data:
            logging.error(f"could not download data for station {name}\n {data}")
            continue

        # rename table column names, or csv column names
        if sensor_mapping:
            ret = do_column_name_mapping(
                name, data["header"], sensor_mapping
            )
            data["header"] = ret if ret is not None else data["header"]

        # build pandas df from data
        df = pd.DataFrame(data["data"])
        df = df.rename(columns=data["header"])

        # Force dateTime always like Datetime
        df.rename(columns={"dateTime": "Datetime"}, inplace=True)

        cols = df.columns.tolist()
        # depending on LOGSTAR_DAYTIME="0"
        # making datetime occure in beginning
        if "Datetime" in cols:
            cols.insert(0, cols.pop(cols.index("Datetime")))
            df = df[cols]

        elif "Date" in cols and "Time" in cols:
            # making date and time occure in beginning
            cols.insert(0, cols.pop(cols.index("Date")))
            cols.insert(0, cols.pop(cols.index("Time")))
            df = df[cols]

        # give data to process
        if processing_steps is not None:
            [df := ps.process(df, name) for ps in processing_steps]

        if df is None or df.empty:
            continue
        
        df = df.rename(columns={"Datetime": datetime_column})

        if database_engine:
            table_name = db_table_prefix + name
           
            if not inspect(database_engine).has_table(table_name=table_name, schema=db_schema):
                logging.info(f"creating database table {table_name} with primary key on {datetime_column} ...")
                pandas_sql = pd.io.sql.pandasSQL_builder(database_engine, schema=db_schema)
                create_table(pandas_sql, df, table_name,
                        index=None, index_label=None, dtype={datetime_column: sq.types.TIMESTAMP(timezone=False)}, keys=datetime_column, if_exists='replace')          

            else:
                contrains = inspect(database_engine).get_pk_constraint(table_name=table_name, schema=db_schema)
                if not "constrained_columns" in contrains or datetime_column not in contrains["constrained_columns"]:
                    logging.error(f"Table {table_name} has no primary key set on {datetime_column} column, this can result in duplicated data in table  ...")

            logging.info("writing {} to database ...".format(table_name))
            to_sql_arugments = {
                "name": table_name,
                "con": database_engine,
                "schema": db_schema,
                "if_exists": 'append',
                "index": False,
                "chunksize": 4096,
                "method": insert_or_do_nothing_on_conflict
            }

            try:
                df.to_sql(**to_sql_arugments)
                logging.info("done writing ...")
            except Exception as E:
                logging.error(f"could not write data to table: {table_name} ...")
                logging.error(E)

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
        ret_data[name] = df
    return ret_data
