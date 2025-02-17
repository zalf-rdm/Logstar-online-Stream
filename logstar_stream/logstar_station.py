
import logging
import re
import json
import re
import requests
import pandas as pd

LOGSTAR_API_URL = "https://logstar-online.de/api"

class LogstarStation:
    FIELDS_TO_IGNORE = ["date", "time"]
    REQUEST_GET_TIMEOUT = 30

    def __init__(self, station, apikey: str, processing_steps=None, mapping=None):
        self.apikey = apikey
        self.station = station
        self.name = station
        if self.mapping:
            self.name = self.__do_sonsor_mapping__(station, self.mapping)
        self.processing_steps = processing_steps
        self.data = None

    def __str__(self):
        return self.name

    def __do_sensor_mapping__(self, station, mapping):
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

    def __do_column_name_mapping__(self, sensor_name, header, mapping):
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
        pattern = re.compile(measurement_class["regex"])

        new_header = {}
        for k, c_name_remote in header.items():
            if c_name_remote in self.FIELDS_TO_IGNORE:
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

    def download_station_data(self,startdate,enddate,datetime=1,geodata=1):
        """
        main routine to download data and save it to database and|or csv
        """
        # docs: https://logstar-online.de/api/{apiKey}/{Stationname}/{StartTag}/{EndTag}/{Channellist}/{DateTime}/{GeoData}

        channel = 0 # all datas
        url = f"{LOGSTAR_API_URL}/{self.apikey}/{self.station}/{startdate}/{enddate}/{channel}/{datetime}/{geodata}"

        logging.debug("downloading {} ...".format(url))
        try:
            r = requests.get(url, timeout=self.REQUEST_GET_TIMEOUT)
        except:
            logging.error(
                f"Error when downloading data for station {self.station} using url {url}...\n{r}"
            )
            self.dataframe = None
            return False
        
        if r.status_code == 200:
            data = json.loads(r.text)
        else:
            logging.error("Request error {}".format(r.status_code))
            self.dataframe = None
            return False

        # no new data or something went wrong while downloading the data
        if data is None or "data" not in data:
            logging.error(f"could not download data for station {self.ame}\n {data}")
            return None

        # rename table column names, or csv column names
        if self.mapping is not None:
            data["header"] = self.__do_column_name_mapping__(
                self.name, data["header"], self.mapping
            )
        
        # build pandas df from data
        df = pd.DataFrame(data["data"])
        # making date and time occure in beginning
        cols = df.columns.tolist()
        cols = cols[-2:] + cols[:-2]
        df = df[cols]
        df = df.rename(columns=data["header"])

        # give data to process
        if self.processing_steps is not None:
            [df := ps.process(df, self.name) for ps in self.processing_steps]

        if df is None or df.empty:
            logging.error(f"no data for station {self.name} ...")
            self.dataframe = None
            return False

        self.dataframe = df
        return True
    
    def get_datframe(self):
        return self.dataframe
    
    def clear_data(self):
        self.dataframe = None
    
    def clear_geodata(self):
        self.geodata = None

    def clear_station(self):
        self.clear_data()
        self.clear_geodata()
