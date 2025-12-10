#!/usr/bin/env python

import argparse
import logging
import json
import sys
import os
import csv
import datetime
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fnmatch import fnmatch
import pandas as pd

import logstar_stream.logstar as logstar
import logstar_stream.processing_steps.ProcessingStep as ps


DEFAULT_DB_SCHEMA = "public"

# env vars which must be set to run logstar stream
REQUIRED_ENV_VARS = ["LOGSTAR_APIKEY"]


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


async def process_station(station, conf, client, processing_steps, sensor_mapping, database_engine, db_schema, db_table_prefix, csv_outfolder, geo_outfolder, executor, rename_datetime_column=None, console_output=False):
    try:
        df = await client.download_station_data(
            conf=conf,
            station=station,
            processing_steps=processing_steps,
            sensor_mapping=sensor_mapping
        )

        if df is not None and not df.empty:
            # Rename datetime column if requested
            if rename_datetime_column:
                if 'date' in df.columns and 'time' in df.columns:
                    # Combine date and time into a single datetime column
                    df[rename_datetime_column] = pd.to_datetime(df['date'] + ' ' + df['time'])
                    df = df.drop(columns=['date', 'time'])
                elif 'Date' in df.columns and 'Time' in df.columns:
                    # Handle capitalized versions
                    df[rename_datetime_column] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
                    df = df.drop(columns=['Date', 'Time'])
                elif 'dateTime' in df.columns:
                    df = df.rename(columns={'dateTime': rename_datetime_column})
                elif 'Datetime' in df.columns:
                    # Handle Datetime with capital D (most common case)
                    df = df.rename(columns={'Datetime': rename_datetime_column})
                else:
                    # If we can't find the expected datetime columns, log a warning
                    logging.warning(f"Could not rename datetime column for station {station}: expected 'date'/'time', 'Datetime', or 'dateTime' columns not found. Available columns: {list(df.columns)}")
            
            # Move datetime column to first position
            datetime_col = None
            if rename_datetime_column and rename_datetime_column in df.columns:
                datetime_col = rename_datetime_column
            elif 'Datetime' in df.columns:
                datetime_col = 'Datetime'
            elif 'dateTime' in df.columns:
                datetime_col = 'dateTime'
            elif 'date' in df.columns and 'time' in df.columns:
                # If we have separate date/time columns, we'll keep them as is
                # but move them to the front
                cols = ['date', 'time'] + [col for col in df.columns if col not in ['date', 'time']]
                df = df[cols]
            
            # Reorder columns to put datetime first
            if datetime_col and datetime_col in df.columns:
                cols = [datetime_col] + [col for col in df.columns if col != datetime_col]
                df = df[cols]
            
            # Console output
            if console_output:
                print(f"\n{'='*80}")
                print(f"Data for station: {station}")
                print(f"{'='*80}")
                print(df.to_string())
                print(f"{'='*80}\n")
            
            # Database write (blocking, so run in executor)
            if database_engine:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    executor,  # Use explicit executor instead of None
                    logstar.write_to_db, 
                    df, 
                    database_engine, 
                    db_schema, 
                    db_table_prefix, 
                    station # Pass station name
                )

            # CSV write
            if csv_outfolder:
                filepath = os.path.join(csv_outfolder, station + ".csv")
                # CSV writing is fast enough or can be blocking, but let's wrap it too if needed.
                # For now, keep it simple.
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

            # GeoJSON write
            if geo_outfolder:
                # This part was broken in original code (self.station usage), skipping fix for now unless requested, 
                # but keeping the block structure.
                pass
                
    except Exception as e:
        logging.error(f"Error processing station {station}: {e}")


async def main_async():
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

    # geojson
    parser.add_argument(
        "-geo",
        "--geo_outdir",
        type=str,
        required=False,
        default=None,
        dest="geo_outfolder",
        help="path to the folder where geo file are stored, if set",
    )

    # console output
    parser.add_argument(
        "--console-output",
        action="store_true",
        dest="console_output",
        default=False,
        help="print downloaded data to the console/command line",
    )

    # station filtering
    parser.add_argument(
        "--filter-stations",
        nargs="+",
        dest="filter_stations",
        default=None,
        help="filter to only process specific stations (space-separated list of station names)",
    )

    # list stations
    parser.add_argument(
        "--list-stations",
        action="store_true",
        dest="list_stations",
        default=False,
        help="list available stations from the API and exit (no data download)",
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
    parser.add_argument(
        "-ps-force",
        "--processing-step-force",
        dest="ps_force",
        action="store_true",
        help="force processing steps to work in ongoing mode, EXPERIMENTAL feature ...",
    )

    # datetime column renaming
    parser.add_argument(
        "--rename-datetime-column",
        type=str,
        dest="rename_datetime",
        required=False,
        default=None,
        help="change name of the Datetime column in the csv files or database tables",
    )

    # db
    parser.add_argument(
        "-db",
        "--database",
        action="store_true",
        dest="enable_database",
        default=False,
        help="enable writing data to database (requires database configuration)",
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
    
    # date range
    parser.add_argument(
        "--start-date",
        type=str,
        dest="start_date",
        default=None,
        help="Start date in YYYY-MM-DD format (overrides LOGSTAR_STARTDATE env var)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        dest="end_date",
        default=None,
        help="End date in YYYY-MM-DD format (overrides LOGSTAR_ENDDATE env var)",
    )
    
    args = parser.parse_args()

    # If no arguments provided (except possibly -v or -l), show help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

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

    # Get start and end dates from command-line args or environment variables
    start_date = args.start_date if args.start_date else os.environ.get("LOGSTAR_STARTDATE")
    end_date = args.end_date if args.end_date else os.environ.get("LOGSTAR_ENDDATE")
    
    # Validate that dates are provided
    if not start_date:
        logging.error("Start date not provided. Use --start-date or set LOGSTAR_STARTDATE environment variable.")
        sys.exit(1)
    if not end_date:
        logging.error("End date not provided. Use --end-date or set LOGSTAR_ENDDATE environment variable.")
        sys.exit(1)

    conf = {
        "apikey": os.environ.get("LOGSTAR_APIKEY"),
        "geodata": os.environ.get("LOGSTAR_GEODATA", "False"),  # Default to False, can be "True" or "False"
        "datetime": os.environ.get("LOGSTAR_DAYTIME", 0),
        "startdate": start_date,
        "enddate": end_date,
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
            with open(args.sensor_mapping, "r") as f:
                sensor_mapping = json.load(f)
            logging.debug(f"loaded sensor mapping: {sensor_mapping}")

    # Validate rename-datetime-column compatibility
    if args.rename_datetime and conf["datetime"] == "1":
        logging.error(
            "ERROR: --rename-datetime-column cannot be used when LOGSTAR_DAYTIME=1.\n"
            "When LOGSTAR_DAYTIME=1, the API returns a single 'dateTime' column.\n"
            "When LOGSTAR_DAYTIME=0, the API returns separate 'date' and 'time' columns.\n"
            "The --rename-datetime-column feature is designed for LOGSTAR_DAYTIME=0 only."
        )
        sys.exit(1)

    # Fetch available stations from API
    # We need to do this inside the async context, so we'll set a flag and do it later
    # For now, we'll just prepare the configuration
    station_list = None  # Will be fetched from API in async context

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
    # Initialize database only if -db flag is set and not in list-stations mode
    if args.enable_database and not args.list_stations:
        database_engine = logstar.init_database(conf=conf)
        if database_engine is None:
            logging.error(
            'provided "db_driver": "{}"  unknown, logstar only supports "PostgreSQL" and "ODBC Driver 17 for SQL Server"...'.format(
                conf["db_driver"])
            )
            sys.exit(1)


    # Create explicit executor for database writes
    executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="db_writer")


    async with logstar.LogstarClient(apikey=conf["apikey"]) as client:
        # Fetch available stations from API
        logging.info("Fetching available stations from API...")
        station_list = await client.get_available_stations(
            start_date=conf["startdate"],
            end_date=conf["enddate"]
        )
        
        # Apply station filtering if requested
        if args.filter_stations:
            original_count = len(station_list)
            filtered_stations = []
            matched_patterns = set()
            
            # Check each station against all filter patterns
            for station in station_list:
                for pattern in args.filter_stations:
                    if fnmatch(station, pattern):
                        filtered_stations.append(station)
                        matched_patterns.add(pattern)
                        break  # Station matched, no need to check other patterns
            
            station_list = filtered_stations
            logging.info(f"Filtered to {len(station_list)} stations (from {original_count} available)")
            
            # Warn about patterns that didn't match any stations
            unmatched_patterns = set(args.filter_stations) - matched_patterns
            if unmatched_patterns:
                logging.warning(f"The following patterns from --filter-stations did not match any stations: {', '.join(unmatched_patterns)}")
        
        # If --list-stations is set, display stations and exit
        if args.list_stations:
            print(f"\n{'='*80}")
            print(f"Available Stations ({len(station_list)} total)")
            print(f"Date Range: {conf['startdate']} to {conf['enddate']}")
            print(f"{'='*80}\n")
            
            # Display in columns for better readability
            for i, station in enumerate(station_list, 1):
                print(f"{i:3d}. {station}")
            
            print(f"\n{'='*80}")
            print(f"Total: {len(station_list)} stations")
            print(f"{'='*80}\n")
            
            logging.info("Station list displayed. Exiting...")
            sys.exit(0)
        
        if not station_list:
            logging.error("No stations to process. Exiting...")
            sys.exit(1)
        
        conf["stationlist"] = station_list
        logging.info(f"Processing {len(station_list)} stations: {', '.join(station_list[:5])}{'...' if len(station_list) > 5 else ''}")
        
        # if ongoing is set logstar constantly looks for new data
        if args.ongoing:
            interval = int(args.interval) * 60
            logging.info(
                "Running in continous mode mit with interval set to: {} seconds ...".format(
                    interval
                )
            )
            
            # Disable processing steps in ongoing mode unless -ps-force is set
            ongoing_processing_steps = None
            if processing_steps:
                if args.ps_force:
                    logging.warning(
                        f'Processing Steps are ENABLED in ongoing mode (EXPERIMENTAL) ...'
                    )
                    ongoing_processing_steps = processing_steps
                else:
                    logging.warning(
                        f'Processing Steps are set, but ignored in "ongoing" mode. Use -ps-force to enable (EXPERIMENTAL) ...'
                    )
            try:
                while True:
                    today = datetime.datetime.today()
                    tomorrow = today + datetime.timedelta(days=1)
                    conf["startdate"] = today.strftime("%Y-%m-%d")  # %H:%M:%S
                    conf["enddate"] = tomorrow.strftime("%Y-%m-%d")
                    
                    tasks = []
                    for station in conf["stationlist"]:
                        tasks.append(process_station(
                            station, conf, client, ongoing_processing_steps, sensor_mapping, 
                            database_engine, db_schema, db_table_prefix, 
                            args.csv_outfolder, args.geo_outfolder, executor, args.rename_datetime, args.console_output
                        ))
                    
                    await asyncio.gather(*tasks)
                    
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logging.warning("interrupted, program is going to shutdown ...")
            except KeyboardInterrupt:
                logging.warning("interrupted, program is going to shutdown ...")
        
        else:
            # download data fro with given parameters: conf, sensor-mapping, database-conn, db-conf, csv-outfolder
            tasks = []
            for station in conf["stationlist"]:
                 tasks.append(process_station(
                    station, conf, client, processing_steps, sensor_mapping, 
                    database_engine, db_schema, db_table_prefix, 
                    args.csv_outfolder, args.geo_outfolder, executor, args.rename_datetime, args.console_output
                ))
            
            await asyncio.gather(*tasks)

    # Shutdown executor with timeout to prevent hanging
    logging.info("Shutting down executor...")
    executor.shutdown(wait=False)  # Don't wait indefinitely
    
    if database_engine:
        logging.info("Closing database connection ...")
        database_engine.dispose()
    logging.info("bye bye ...")
    
    # Force exit to prevent hanging
    os._exit(0)


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.warning("Interrupted by user, forcing exit...")
        os._exit(0)  # Force immediate exit without cleanup

if __name__ == "__main__":
    main()
