#!/usr/bin/env python

import argparse
import logging
import json
import sys
import os
import datetime
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

import logstar_stream.logstar as logstar
import logstar_stream.processing_steps.ProcessingStep as ps

DB_RECONNECT_TIMEOUT = 3  # time in between reconnect attempts

DEFAULT_DB_SCHEMA = "public"

# env vars which must be set to run logstar stream
REQUIRED_ENV_VARS = ["LOGSTAR_APIKEY", "LOGSTAR_STATIONS"]


def configure_logging(debug, filename=None):
    """define loglevel and log to file or to std"""
    if filename is None:
        if debug:
            logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)
        else:
            logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    else:
        if debug:
            logging.basicConfig(
                filename=filename, format="%(asctime)s %(message)s", level=logging.DEBUG
            )
        else:
            logging.basicConfig(
                filename=filename, format="%(asctime)s %(message)s", level=logging.INFO
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--ongoing",
        dest="ongoing",
        action="store_true",
        help="activate continous downloading new released data on logstar-online for given stations",
    )
    parser.add_argument(
        "-i", "--interval", type=int, default=20, help="sampling interval in minutes"
    )
    parser.add_argument(
        "-m",
        "--sensor_mapping_file",
        type=str,
        dest="sensor_mapping",
        help="path to json file for raw sensor name to tablename mapping",
    )

    # csv
    parser.add_argument(
        "-co",
        "--csv_outdir",
        type=str,
        required=False,
        default=None,
        dest="csv_outfolder",
        help="path to the folder where csv file are stored, if set",
    )

    # plugins
    parser.add_argument(
        "-ps",
        "--processing-step",
        dest="ps",
        nargs="+",
        action="append",
        help="adds a processingstep to work on downloaded data. This only applies if ongoing is not set",
    )

    # db
    parser.add_argument(
        "-nodb",
        "--disable_database",
        action="store_true",
        dest="disable_database",
        default=False,
        help="with -nodb set, results in no interaction with the database",
    )
    parser.add_argument(
        "-dbtp",
        "--db_table_prefix",
        dest="db_table_prefix",
        type=str,
        required=False,
        help="Prefix set for tables in Database",
    )
    parser.add_argument(
        "-dbs",
        "--db_schema",
        dest="db_schema",
        default=DEFAULT_DB_SCHEMA,
        required=False,
        type=str,
        help="Database schema",
    )

    # logging
    parser.add_argument(
        "-l",
        "--log",
        help="Redirect logs to a given file in addition to the console.",
        metavar="",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    args = parser.parse_args()

    debug = False
    if args.verbose:
        debug = True

    if args.log:
        logfile = args.log
        configure_logging(debug, logfile)
    else:
        configure_logging(debug)
        logging.debug("debug mode enabled")

    logging.debug("reading configuration from OS environment ...")

    for re in REQUIRED_ENV_VARS:
        if os.environ.get(re) is None:
            logging.error(f"Required env var {re} is not set, bye ...")
            sys.exit(1)

    conf = {
        "apikey": os.environ.get("LOGSTAR_APIKEY"),
        "stations": os.environ.get("LOGSTAR_STATIONS"),
        "geodata": os.environ.get("LOGSTAR_GEODATA", True),
        "datetime": os.environ.get("LOGSTAR_DAYTIME", 0),
        "startdate": os.environ.get("LOGSTAR_STARTDATE", "2021-01-01"),
        "enddate": os.environ.get("LOGSTAR_ENDDATE", "2021-05-02"),
        "db_host": os.environ.get("LOGSTAR_DB_HOST", "localhost"),
        "db_database": os.environ.get("LOGSTAR_DB_DBNAME", "logstar"),
        "db_driver": os.environ.get("LOGSTAR_DB_DRIVER", "PostgreSQL"),
        "db_username": os.environ.get("LOGSTAR_DB_USER", "postgres"),
        "db_password": os.environ.get("LOGSTAR_DB_PASS", "postgres"),
        "db_port": os.environ.get("LOGSTAR_DB_PORT", "5432"),
    }

    logging.debug("loaded environment variables:")
    [logging.debug('\t{} -> "{}"'.format(key, value)) for key, value in conf.items()]

    # load sensor mapping file for mapping station names to names given in sensor-mapping file
    sensor_mapping = None
    if args.sensor_mapping:
        if os.path.exists(args.sensor_mapping):
            with open(args.sensor_mapping, "r") as jsonfile:
                jsonfile_contents = jsonfile.read()
                sensor_mapping = json.loads(jsonfile_contents)
            logging.debug(f"Found sensor mapping json under: {args.sensor_mapping}")

    # splits station names from conf given as space seperated string to list
    try:
        station_list = conf["stations"].split(" ")
        conf["stationlist"] = station_list
    except:
        logging.error("Could not seperate stations bye bye...")
        sys.exit(1)

    # checks given csv_outfolder path
    if args.csv_outfolder is not None:
        if not os.path.exists(args.csv_outfolder):
            logging.error(
                "provided csv path: {} does not exist, bye ...".format(
                    args.csv_outfolder
                )
            )
            sys.exit(1)
        logging.info("found csv folder: %s ..." % args.csv_outfolder)

    processing_steps = None
    # check and init processing steps
    if args.ps:
        processing_steps = [
            ps.load_class(ps_step_and_args) for ps_step_and_args in args.ps
        ]

    # set db schema
    db_schema = args.db_schema

    # set db table prefix
    db_table_prefix = args.db_table_prefix if args.db_table_prefix is not None else ""

    database_engine = None
    # skip database driver evaluation if -nodb set
    if not args.disable_database:
        # test database connection
        if conf["db_driver"] == "PostgreSQL":
            connection_url = URL.create(
                "postgresql",
                username=conf["db_username"],
                password=conf["db_password"],
                host=conf["db_host"],
                port=conf["db_port"],
                database=conf["db_database"],
            )
            database_engine = create_engine(connection_url)

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
            logging.error(
                'provided "db_driver": "{}"  unknown, logstar only supports "PostgreSQL" and "ODBC Driver 17 for SQL Server"...'.format(
                    conf["db_driver"]
                )
            )
            sys.exit(1)

        # try connect to database
        i = 0
        while True:
            database_engine = create_engine(connection_url)
            if not database_engine:
                logging.error(
                    "Could not connect to database, retry number {} ...".format(i)
                )
                i += 1
                time.sleep(DB_RECONNECT_TIMEOUT)
            else:
                break

    # if ongoing is set logstar constantly looks for new data
    if args.ongoing:
        interval = int(args.interval) * 60
        logging.info(
            "Running in continous mode mit with interval set to: {} seconds ...".format(
                interval
            )
        )
        if processing_steps:
            logging.warning(
                f'Processing Steps are set, but currently ignored in "ongoing" mode ...'
            )
        try:
            while True:
                today = datetime.datetime.today()
                tomorrow = today + datetime.timedelta(days=1)
                conf["startdate"] = today.strftime("%Y-%m-%d")  # %H:%M:%S
                conf["enddate"] = tomorrow.strftime("%Y-%m-%d")
                logstar.manage_dl_db(
                    conf,
                    database_engine,
                    sensor_mapping=sensor_mapping,
                    db_schema=db_schema,
                    db_table_prefix=db_table_prefix,
                )
                time.sleep(interval)
        except KeyboardInterrupt:
            logging.warning("interrupted, program is going to shutdown ...")
    else:
        # download data fro with given parameters: conf, sensor-mapping, database-conn, db-conf, csv-outfolder
        logstar.manage_dl_db(
            conf,
            database_engine,
            processing_steps=processing_steps,
            sensor_mapping=sensor_mapping,
            csv_folder=args.csv_outfolder,
            db_schema=db_schema,
            db_table_prefix=db_table_prefix,
        )

    if database_engine:
        logging.info("Closing database connection ...")
        database_engine.disconnect()
    logging.info("bye bye ...")


if __name__ == "__main__":
    main()
