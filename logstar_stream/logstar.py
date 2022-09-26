

import requests
import re
import logging
import json
import os
import csv
from typing import List

import pandas as pd

'''
	API DOCs
	http://dokuwiki.weather-station-data.com/doku.php?id=:en:start
'''
LOGSTAR_API_URL = "https://logstar-online.de/api"


def build_url(conf, station, channel):
    ''' build url to request from 
        docs: https://logstar-online.de/api/{apiKey}/{Stationname}/{StartTag}/{EndTag}/{Channellist}/{DateTime}/{GeoData}
    '''
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
    ''' get readable name for given station from mapping '''
    for key, value in mapping['sensor-mapping'].items():
        if value["value"] in station:
            return key
    logging.debug("Mapping for sensor {} not found ...".format(station))
    return station


FIELDS_TO_IGNORE = ["date", "time"]


def do_column_name_mapping(sensor_name, header, mapping):
    if sensor_name not in mapping["sensor-mapping"] or not mapping['sensor-mapping'][sensor_name]:
        logging.info(
            "could not provide measurement mapping for sensor {}, not found ...".format(sensor_name))
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
                if "only_includes_abbreviation" in value and value["only_includes_abbreviation"]:
                    new_header[k] = c_name = name
                    continue

                r = pattern.match(c_name_remote)
                if "*" in measurement_class["position"]:
                    c_name = "{}_{}_{}_cm".format(name,
                                                  measurement_class["position"]["*"]["side"],
                                                  measurement_class["position"]["*"]["depth"],
                                                  )
                else:
                    c_name = "{}_{}_{}_cm".format(name,
                                                  measurement_class["position"][r["number"]]["side"],
                                                  measurement_class["position"][r["number"]]["depth"]
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


def download_data(conf, station):
    url = build_url(conf, station=station, channel=1)
    request = request_data(url)
    if request is None:
        return None
    number_of_channels = 1
    try:
        dict_request = json.loads(request)
        number_of_channels = len(
            dict_request["header"].keys()) - 1  # - time - date
    except:
        logging.error(
            "Could not calculate number of channels for station {}. Request may be broken ...".format(station))
        return None

    # who are you, starting to count with 1?
    # building channel string for build_url
    channels = ','.join(map(str, range(1, number_of_channels)))
    url = build_url(conf, station=station, channel=channels)
    request = request_data(url)
    if request is None:
        return None
    return json.loads(request)


def manage_dl_db(conf, database_engine=None, processing_steps: List = [], sensor_mapping=None, csv_folder=None, db_schema=None, db_table_prefix=None):
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
        logging.info("downloading data for station {} from {} to {} ...".format(
            name, conf["startdate"], conf["enddate"]))

        # download data
        data = download_data(conf, station)
        if data is None:
            # no new data or something went wrong while downloading the data
            continue

        # rename table column names, or csv column names
        if sensor_mapping is not None:
            data['header'] = do_column_name_mapping(name, data['header'], sensor_mapping)

        # build pandas df from data
        df = pd.DataFrame(data['data'])
        df = df.rename(columns=data['header'])

        # give data to process
        if processing_steps is not None:
            [df := ps.process(df, name) for ps in processing_steps]

        if df is None or df.empty:
            continue

        if database_engine:
            table_name = db_table_prefix + name
            logging.info("writing {} to database ...".format(table_name))
            df.to_sql(table_name,
                      con=database_engine,
                      schema=db_schema,
                      if_exists="append")

        # write to file
        if csv_folder:
            filepath = os.path.join(csv_folder, name + ".csv")
            df.to_csv(filepath,
                      sep=',',
                      quotechar='"',
                      header=True,
                      doublequote=False,
                      quoting=csv.QUOTE_MINIMAL,
                      index=False
                      )
        ret_data[name] = df
    return ret_data
