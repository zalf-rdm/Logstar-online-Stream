
from re import M
from typing import List, Dict
import math
import logging

import pandas as pd

from processing_steps.ProcessingStep import ProcessingStep


class EnvObject():
  def __init__(self):
    self.last_value = float("NaN")
    self.in_false_state = False


class JumpCheckPS(ProcessingStep):

  ps_name = "JumpCheckPlugin"

  # measurements to check for data jumps
  JUMP_CHECK_COLUMN_NAMES = ['water_content_right_60_cm']#['water_content_left_30_cm', 'water_content_left_60_cm', 'water_content_left_90_cm'
                             #'water_content_right_30_cm', 'water_content_right_60_cm', 'water_content_right_90_cm']

  # value to fill if missmeasurement detected
  ERROR_VALUE = float("NaN")

  # maximum duration of a jump, if it takes longer than this the data will not be deleted in messurement
  MAXIMUM_JUMP_DURATION = 5

  # jump difference between two following values
  MINIMUM_JUMP_DIFFER_VALUE = 5

  def __init__(self, args: Dict):
    super().__init__(args)
    self.env = {}

  # remove jumps up 5 % for a single measurement
  def process(self, df: pd.DataFrame, station: str, argument: List = None):
    logging.debug(f"parsing data for station {station} ...")
    for index, row in df.iterrows():
      # get through all defined measurements in JUMP_CHECK_MEASUREMENTS
      for column in self.JUMP_CHECK_COLUMN_NAMES:
        if column not in row:
          continue
        
        object_identifier = station + "_" + column
        station_messurement_env = EnvObject() if object_identifier not in self.env else self.env[object_identifier]     
        current_value = row[column]

        # check if current_value is nan
        if math.isnan(current_value):
          station_messurement_env.last_value = float("NaN")
          continue

        # jump up or false state active
        if current_value - station_messurement_env.last_value >= self.MINIMUM_JUMP_DIFFER_VALUE or station_messurement_env.in_false_state == True:
          station_messurement_env.in_false_state = True
          df.at[index, column] = self.ERROR_VALUE
          print(f"diff: {abs(current_value - station_messurement_env.last_value)}, {current_value}, {station_messurement_env.last_value}, {df.at[index, 'date']}, {df.at[index, 'time']}")
          print(f"A: ERROR, {station} index: {index} column: {column}")
          breakpoint()
        # Jump Down
        elif station_messurement_env.last_value - current_value >= self.MINIMUM_JUMP_DIFFER_VALUE:
          print(f"B:ERROR, {station} index: {index} column: {column}")
          breakpoint()
          df.at[index, column] = self.ERROR_VALUE
          station_messurement_env.in_false_state = False

        station_messurement_env.last_value = current_value
        self.env[object_identifier] = station_messurement_env
    return df
