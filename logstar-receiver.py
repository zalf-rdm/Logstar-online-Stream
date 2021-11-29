#!/usr/bin/env python

import argparse
import requests

import time
from datetime import datetime, timedelta
import logging
import os
import json
import sys
from src.db_connector import MSSQLConnector,PSQLConnector 
'''
	API DOCs
	http://dokuwiki.weather-station-data.com/doku.php?id=:en:start 
'''
LOGSTAR_API_URL = "https://logstar-online.de/api"


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

def read_conf_from_file(filename):
	raise NotImplementedError("This function is not yet implemented, please use environment variables to pass configuration values ...")

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

def do_mapping(station, mapping):
	for key, value in mapping.items():
		print(value["value"], station)
		if value["value"] in station:
			return key
	logging.debug("Mapping for {} not found ...".format(station))
	return station

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
		number_of_channels = len(dict_request["header"].keys()) - 2 # - time - date
	except:
		logging.debug("Could not calculate number of channels for station {}. Request may be broken ...".format(station))
		return None

	channels=','.join(map(str,range(1,number_of_channels))) # who are you starting to count with 1
	url = build_url(conf,station=station,channel=channels)
	request = request_data(url)
	if request is None:
		return None
	return json.loads(request)

def manage_dl_db(conf,database, mapping=None):
	for station in conf["stationlist"]:
		name = station
		if mapping:
			name = do_mapping(station, mapping)
		logging.info("downloading data for station {} from {} to {} ...".format(name,conf["startdate"],conf["enddate"]))
		dict_request = download_data(conf,station)
		if dict_request is None:
			continue

		database.create_table(name,dict_request['header'])
		database.insert_data(name,dict_request)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-c","--config",type=argparse.FileType('r', encoding='UTF-8'),help="pass config via config file, default via env")
	#parser.add_argument("-dry-run",type=argparse.FileType('r', encoding='UTF-8'),help="dry run downloads data but does not interact with database ...")
	parser.add_argument("-o","--ongoing",action='store_true',help="activate continous downloading new released data on logstar-online for given stations")
	parser.add_argument("-i","--interval",type=int,default=20,help="sampling interval in minutes")
	parser.add_argument("-m","--mapping-file", type=str,dest="mapping", help="path to json file for raw data to tablename mapping")

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

	if args.config:
		conf = read_conf_from_file(args.c)
	else:
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
		
		mapping = None
		if args.mapping:
			if os.path.exists(args.mapping):
				with open(args.mapping) as jsonfile:
					mapping = json.load(jsonfile)
				logging.debug("Found mapping json under: {} with following mapping:\n {}".format(args.mapping, mapping))

		try:
			station_list = conf["stations"].split(" ")
			conf["stationlist"] = station_list
		except:
			logging.error("Could not seperate stations bye bye...")
			sys.exit(1)
	
	# test database connection
	if conf["db_driver"] == "PostgreSQL":
		database = PSQLConnector(conf)
	else:
		database = MSSQLConnector(conf)
	
	if not database.connect():
		sys.exit(1)

	if args.ongoing:
		interval = int(args.interval) * 60
		logging.info("Running in continous mode mit with interval set to: {} seconds ...".format(interval))
		try:
			while True:
				today = datetime.today()
				yesterday = today - timedelta(days=1)
				tomorrow = today + timedelta(days=1)
				now = datetime.now()
				before = now - timedelta(seconds=(interval*2))
				conf["startdate"] = today.strftime('%Y-%m-%d') # %H:%M:%S
				conf["enddate"] = tomorrow.strftime('%Y-%m-%d')
				manage_dl_db(conf,database, mapping=mapping)
				time.sleep(interval)
		except KeyboardInterrupt:
			logging.warning('interrupted, program is going to shutdown ...')
	else:
		manage_dl_db(conf, database, mapping=mapping)
	
	logging.info("Closing database connection ...")
	database.disconnect()

if __name__ == '__main__':
	main()