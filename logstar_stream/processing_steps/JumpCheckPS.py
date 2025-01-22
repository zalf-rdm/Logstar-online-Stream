from typing import List, Dict
import math
import logging

import pandas as pd

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class EnvObject:
    """
    helper class object to keep track of status for a given meassurement/station combination
    while iter through dataframes
    """

    def __init__(self):
        self.last_value = float("NaN")
        self.to_change = []
        self.jump_duration = 0

    def __str__(self):
        return f"Last value: {self.last_value}, to change: {self.to_change}, jump duration: {self.jump_duration}"


class JumpCheckPS(ProcessingStep):
    ps_name = "JumpCheckPlugin"

    ps_description = """
        This processing step looks for jumps in water_content messurements (JUMP_CHECK_COLUMN_NAMES). If the between two
        meassurements the values jumps higher than a given Value(MINIMUM_JUMP_DIFFER_VALUE) and jumps back in a given amount
        of meassurements (MAXIMUM_JUMP_DURATION) we delete the so called jump data as we assume that it is missmeassurement
        from the sensor
        """

    # measurements to check for data jumps
    JUMP_CHECK_COLUMN_NAMES = [
        "water_content_left_30_cm",
        "water_content_left_60_cm",
        "water_content_left_90_cm",
        "water_content_right_30_cm",
        "water_content_right_60_cm",
        "water_content_right_90_cm",
    ]

    # value to fill if missmeasurement detected
    ERROR_VALUE = pd.NA

    # maximum duration of a jump, if it takes longer than this the data will not be deleted in messurement
    MAXIMUM_JUMP_DURATION = 10.0

    # jump difference between two following values
    MINIMUM_JUMP_DIFFER_VALUE = 5.0

    def __init__(self, kwargs):
        super().__init__(kwargs)
        self.env = {}

    def change_values(self, df, station_messurement_env):
        """
        Change values in the given dataframe based on the station measurement environment.

        Parameters:
            df (pandas.DataFrame): The dataframe to modify.
            station_messurement_env (StationMeasurementEnvironment): The object containing information about the station measurement environment.

        Returns:
            None
        """
        [
            df := self.__do_change__(df, row_num, column_name)
            for row_num, column_name in station_messurement_env.to_change
        ]

    def reset_counters(self, station_messurement_env):
        """
        Reset the counters of the station measurement environment.

        Parameters:
            station_messurement_env (object): The station measurement environment object.

        Returns:
            None
        """
        station_messurement_env.jump_duration = 0
        station_messurement_env.to_change = []

    def process(self, df: pd.DataFrame, station: str):
        """
        remove jumps up 5 % for a single measurement

        :param df
        :param station
        :param argument

        :return df
        """
        logging.debug(f"parsing data for station {station} ...")
        if df is None:
            return

        for index, row in df.iterrows():
            # get through all defined measurements in JUMP_CHECK_MEASUREMENTS
            for column in self.JUMP_CHECK_COLUMN_NAMES:
                if column not in row:
                    continue

                object_identifier = station + "_" + column
                station_messurement_env = (
                    EnvObject()
                    if object_identifier not in self.env
                    else self.env[object_identifier]
                )
                current_value = row[column]

                # check if current_value is nan
                if current_value is None or pd.isnull(current_value):
                    self.reset_counters(station_messurement_env)
                    station_messurement_env.last_value = pd.NA
                    continue

                if pd.isnull(station_messurement_env.last_value):
                    station_messurement_env.last_value = current_value
                    continue

                # Jump Down
                if (
                    station_messurement_env.last_value - current_value
                    >= self.MINIMUM_JUMP_DIFFER_VALUE
                ):
                    # print("DOWN",row['date'],row['time'],column,current_value)
                    # after data jumped down change effected values
                    self.change_values(df, station_messurement_env)
                    self.reset_counters(station_messurement_env)

                # jump up or false state active
                elif (
                    current_value - station_messurement_env.last_value
                    >= self.MINIMUM_JUMP_DIFFER_VALUE
                    or station_messurement_env.jump_duration > 0
                ):
                    # print("UP",row['date'],row['time'],column,current_value)
                    station_messurement_env.jump_duration += 1
                    # add value to change
                    station_messurement_env.to_change.append((index, column))

                # if jump goes on for longer, then we want to keep the data as it is
                if station_messurement_env.jump_duration >= self.MAXIMUM_JUMP_DURATION:
                    # print("CONTINUE",row['date'],row['time'],column,current_value)
                    self.reset_counters(station_messurement_env)

                station_messurement_env.last_value = current_value
                self.env[object_identifier] = station_messurement_env
        self.write_log(station)
        self.changed = []
        return df
