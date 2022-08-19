
from re import M
from typing import List, Dict
import math

import pandas as pd

from processing_steps.ProcessingStep import ProcessingStep


class EnvObject():
  def __init__(self):
    self.last_value = float("NaN")
    self.in_false_state = False

class JumpCheckPS(ProcessingStep):

    ps_name = "JumpCheckPlugin"

    # measurements to check for data jumps
    JUMP_CHECK_COLUMN_NAMES = ['water_content_left_30_cm', 'water_content_left_60_cm', 'water_content_left_90_cm'
                               'water_content_right_30_cm', 'water_content_right_60_cm', 'water_content_right_90_cm']

    # value to fill if missmeasurement detected
    ERROR_VALUE = float("NaN")

    # jump difference between two following values
    MAXIMUM_JUMP_DIFFER_VALUE = 5

    def __init__(self, args: Dict):
      super().__init__(args)
      self.env = {}

    # remove jumps up 5 % for a single measurement
    def process(self, df: pd.DataFrame, station: str, arguments: List):

        station_env = EnvObject() if station not in self.env else self.env[station]
        for index, row in df.iterrows():
        
          # get through all defined measurements in JUMP_CHECK_MEASUREMENTS
          for column in self.JUMP_CHECK_COLUMN_NAMES:
              current_value = row[column]

              # check if current_value is nan
              if math.isnan(current_value):
                  continue
              
              # jump up or false state active
              if current_value - station_env.last_value >= self.MAXIMUM_JUMP_DIFFER_VALUE or station_env.in_false_state == True:
                station_env.in_false_state = True
                df.at[index, column] = self.ERROR_VALUE
                print(f"ERROR, {station} index: {index} column: {column}")

              # Jump Down
              elif station_env.last_value - current_value >= self.MAXIMUM_JUMP_DIFFER_VALUE:
                print(f"ERROR, {station} index: {index} column: {column}")
                df.at[index, column] = self.ERROR_VALUE
                station_env.in_false_state = False
              
              station_env.last_value = current_value
          self.env[station] = station_env