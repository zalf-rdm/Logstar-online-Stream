#!/usr/bin/env python

import argparse
import requests
import time
from datetime import datetime, timedelta
import logging
import json
import sys
import os

from src.db_connector import MSSQLConnector,PSQLConnector
import src.csv_handler as csv_handler

'''
	API DOCs
	http://dokuwiki.weather-station-data.com/doku.php?id=:en:start 
'''
LOGSTAR_API_URL = "https://logstar-online.de/api"

DB_RECONNECT_TIMEOUT = 3 # time in between reconnect attempts

def configure_logging(debug,filename=None):
	if filename is None:
		if debug:
			logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
		else:
			logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
	else:
		if debug:
			logging.basicConfig(filename=filename,format='%(asctime)s %(message)s', level=logging.DEBUG)
		else:
			logging.basicConfig(filename=filename,format='%(asctime)s %(message)s', level=logging.INFO)


''' build url to request from '''
def build_url(conf,station,channel):
	# docs: https://logstar-online.de/api/{apiKey}/{Stationname}/{StartTag}/{EndTag}/{Channellist}/{DateTime}/{GeoData} 
	url = "{}/{}/{}/{}/{}/{}/{}/{}".format(
							LOGSTAR_API_URL,
							conf["apikey"],
							station,
							conf["startdate"],
							conf["enddate"],
							channel,
							conf["datetime"],
							conf["geodata"]
						)
	return url

def do_sensor_mapping(station, mapping):
	for key, value in mapping['sensor-mapping'].items():
		if value["value"] in station:
			return key
	logging.debug("Mapping for sensor {} not found ...".format(station))
	return station

FIELDS_TO_IGNORE = ["date", "time"]

def do_measurement_mapping(sensor_name, header, mapping):
	import re
	if sensor_name not in mapping["sensor-mapping"] or not mapping['sensor-mapping'][sensor_name]:
		logging.info("could not provide measurement mapping for sensor {}, not found ...".format(sensor_name))
		return header

	measurement_class_name = mapping["sensor-mapping"][sensor_name]["measurement-class"]
	measurement_class = mapping["measurement-classes"][measurement_class_name]
	pattern = re.compile(measurement_class["regex"])
	
	new_header = {}
	for k ,c_name_remote in header.items():
		print(c_name_remote)
		if c_name_remote in FIELDS_TO_IGNORE:
			new_header[k] = c_name_remote
			continue
		for name, value in measurement_class["mapping"].items():
			if value["abbreviation"] in c_name_remote:
				if "only_includes_abbreviation" in value and value["only_includes_abbreviation"]:
					c_name = "{}_-_{}".format(name, value["unit"])
					new_header[k] = c_name
					continue

				r = pattern.match(c_name_remote)
				if "*" in measurement_class["position"]:
					c_name = "{}_{}_{}_cm_{}".format(name, 
																					measurement_class["position"]["*"]["side"], 
																					measurement_class["position"]["*"]["depth"],
																					value["unit"]
																					)
				else:
					c_name = "{}_{}_{}_cm_{}".format(name, 
																					measurement_class["position"][r["number"]]["side"], 
																					measurement_class["position"][r["number"]]["depth"], 
																					value["unit"]
																					)
				new_header[k] = c_name
	return new_header

def request_data(url):
	logging.debug("requesting {} ...".format(url))
	try:
		r = requests.get(url)
	except:
		return None
	if r.status_code == 200:
		return r.text
	else:
		logging.debug("Request error {}".format(r.status_code))
		return None

def download_data(conf,station):
	url = build_url(conf,station=station,channel=1)
	request = request_data(url)
	if request is None:
		return None
	number_of_channels = 1
	try:
		dict_request = json.loads(request)
		number_of_channels = len(dict_request["header"].keys()) - 1 # - time - date
	except:
		logging.debug("Could not calculate number of channels for station {}. Request may be broken ...".format(station))
		return None

	channels=','.join(map(str,range(1,number_of_channels))) # who are you starting to count with 1
	url = build_url(conf,station=station,channel=channels)
	request = request_data(url)
	if request is None:
		return None
	return json.loads(request)

def manage_dl_db(conf, database, sensor_mapping=None, csv_folder=None, db_schema=None, db_table_prefix=None):
	for station in conf["stationlist"]:
		name = station
		if sensor_mapping:
			name = do_sensor_mapping(station, sensor_mapping)		
		logging.info("downloading data for station {} from {} to {} ...".format(name,conf["startdate"],conf["enddate"]))
		dict_request = download_data(conf,station)
		if dict_request is None:
			continue
		dict_request['header'] = do_measurement_mapping(name, dict_request['header'], sensor_mapping)

		if database is not None:
			db_schema_table_name = database.build_schema_table_name(table_basename=name, prefix=db_table_prefix, schema=db_schema)
			database.create_table(db_schema_table_name,dict_request['header'])
			database.insert_data(db_schema_table_name,dict_request)

		if csv_folder:				
			csv_handler.write_csv_file(csv_folder, name, dict_request)
	return

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-o","--ongoing",action='store_true',help="activate continous downloading new released data on logstar-online for given stations")
	parser.add_argument("-i","--interval",type=int,default=20,help="sampling interval in minutes")
	parser.add_argument("-m","--sensor_mapping_file", type=str,dest="sensor_mapping", help="path to json file for raw sensor name to tablename mapping")
	
	# csv
	parser.add_argument("-co","--csv_outdir", type=str, required=False, default=None, dest="csv_outfolder", help="path to the folder where csv file are stored, if set")

	# db
	parser.add_argument("-nodb", "--disable_database", action='store_true', dest="disable_database", default=False, help="with -nodb set, results in no interaction with the database")
	parser.add_argument("-dbtp", "--db_table_prefix",dest="db_table_prefix", type=str, required=False, help="Prefix set for tables in Database")
	parser.add_argument("-dbs", "--db_schema",dest="db_schema", required=False, type=str, help="Database schema")

	# logging
	parser.add_argument("-l","--log",help="Redirect logs to a given file in addition to the console.",metavar='')
	parser.add_argument("-v","--verbose",action='store_true',help="Enable verbose logging")
	args = parser.parse_args()

	debug = False
	if args.verbose:
		debug = True

	if args.log:
		logfile = args.log
		configure_logging(debug,logfile)
	else:
		configure_logging(debug)
		logging.debug("debug mode enabled")

	logging.debug("reading configuration from OS environment ...")
	conf = {
		"apikey": os.environ.get('LOGSTAR_APIKEY'),
		"stations":os.environ.get('LOGSTAR_STATIONS'),
		"geodata": os.environ.get('LOGSTAR_GEODATA',True),
		"datetime": os.environ.get('LOGSTAR_DAYTIME',0),
		"startdate": os.environ.get('LOGSTAR_STARTDATE',"2021-01-01"),
		"enddate": os.environ.get('LOGSTAR_ENDDATE',"2021-05-02"),
		"db_host": os.environ.get('LOGSTAR_DB_HOST','localhost'),
		"db_database": os.environ.get('LOGSTAR_DB_DBNAME','logstar'),
		"db_driver": os.environ.get('LOGSTAR_DB_DRIVER','PostgreSQL'),
		"db_username": os.environ.get('LOGSTAR_DB_USER','postgres'),
		"db_password": os.environ.get('LOGSTAR_DB_PASS','postgres'),
		"db_port": os.environ.get('LOGSTAR_DB_PORT','5432')
	}
	logging.debug("loaded environment variables:")
	for key,value in conf.items():
		logging.debug("\t{} -> \"{}\"".format(key,value))

	sensor_mapping = None
	if args.sensor_mapping:
		if os.path.exists(args.sensor_mapping):
			with open(args.sensor_mapping) as jsonfile:
				sensor_mapping = json.load(jsonfile)
			logging.debug("Found sensor mapping json under: {} with following mapping:\n {}".format(args.sensor_mapping, sensor_mapping))

	try:
		station_list = conf["stations"].split(" ")
		conf["stationlist"] = station_list
	except:
		logging.error("Could not seperate stations bye bye...")
		sys.exit(1)

	if args.csv_outfolder is not None:
		if not csv_handler.folder_exists(args.csv_outfolder):
			logging.error("provided csv path: {} does not exist, bye ...".format(args.csv_outfolder))
			sys.exit(1)
		logging.info("found csv folder: %s ..." % args.csv_outfolder)

	db_schema = None
	if args.db_schema:
		db_schema = args.db_schema

	db_table_prefix = ""
	if args.db_table_prefix:
		db_table_prefix = args.db_table_prefix

	database = None
	# skip database driver evaluation if -nodb set
	if not args.disable_database:	
		# test database connection
		if conf["db_driver"] == "PostgreSQL":
			database = PSQLConnector(conf)
		elif conf["db_driver"] == "ODBC Driver 17 for SQL Server":
			database = MSSQLConnector(conf)
		else:
			logging.error("provided \"db_driver\": \"{}\"  unknown, logstar only supports \"PostgreSQL\" and \"ODBC Driver 17 for SQL Server\"...".format( conf["db_driver"]))
			sys.exit(1)
		
		# try connect to database
		i = 0
		while True:
			if not database.connect():
				logging.error("Could not connect to database, retry number {} ...".format(i))
				i += 1
				time.sleep(DB_RECONNECT_TIMEOUT)
			else:
				break

	if args.ongoing:
		interval = int(args.interval) * 60
		logging.info("Running in continous mode mit with interval set to: {} seconds ...".format(interval))
		try:
			while True:
				today = datetime.today()
				tomorrow = today + timedelta(days=1)
				conf["startdate"] = today.strftime('%Y-%m-%d') # %H:%M:%S
				conf["enddate"] = tomorrow.strftime('%Y-%m-%d')
				manage_dl_db(conf, database, sensor_mapping=sensor_mapping, db_schema=db_schema, db_table_prefix=db_table_prefix)
				time.sleep(interval)
		except KeyboardInterrupt:
			logging.warning('interrupted, program is going to shutdown ...')
	else:
		manage_dl_db(conf, database, sensor_mapping=sensor_mapping, csv_folder=args.csv_outfolder, db_schema=db_schema, db_table_prefix=db_table_prefix)

	if database:
		logging.info("Closing database connection ...")
		database.disconnect()
	logging.info("bye bye ...")
if __name__ == '__main__':
	main()