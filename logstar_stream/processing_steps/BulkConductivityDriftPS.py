from re import M
from typing import List, Dict
import math
import logging

import pandas as pd

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class BulkConductivityDriftPS(ProcessingStep):
    ps_name = "BulkConductivityDriftPS"

    ps_description = "TODO"

    # value to fill if missmeasurement detected
    ERROR_VALUE = float("NaN")

    FORBIDDEN_VALUES = [{"value": 0, "duration": 100}]

    treshold_left_to_right = 50
    threshold_between_depth = 60

    ELEMENT_ORDER_LEFT = [
        "bulk_conductivity_left_30_cm",
        "bulk_conductivity_left_60_cm",
        "bulk_conductivity_left_90_cm",
    ]
    ELEMENT_ORDER_RIGHT = [
        "bulk_conductivity_right_30_cm",
        "bulk_conductivity_right_60_cm",
        "bulk_conductivity_right_90_cm",
    ]

    def __init__(self, kwargs):
        super().__init__(kwargs)
        self.treshold_left_to_right = float(kwargs['treshold_left_to_right']) if "treshold_left_to_right" in kwargs else self.treshold_left_to_right
        self.threshold_between_depth = float(kwargs['threshold_between_depth']) if "threshold_between_depth" in kwargs else self.threshold_between_depth

        self.to_change = []

    def compare_and_prepare_to_change(self, column_identifiers_left, column_identifiers_right, row_num, row, i):
        left_value = row[column_identifiers_left[i]]
        right_value = row[column_identifiers_right[i]]
        
        if None in (left_value, right_value) or math.isnan(left_value) or math.isnan(right_value):
            return

        left_del = False
        right_del = False

        # compare diff between left and right side. If left or right higher than treshold_left_to_right + (left or right) remove the other
        if left_value - right_value > self.treshold_left_to_right:
            left_del = True
            self.to_change.append((int(row_num), column_identifiers_left[i]))
            #print("LR", left_value, right_value, ":", left_value)

        elif right_value - left_value > self.treshold_left_to_right:
            right_del = True
            self.to_change.append((int(row_num), column_identifiers_right[i]))
            #print("LR",left_value, right_value, ":", right_value)

        if i == 1:
          return
        
        # check distance between depth and next deph is lower than threshold_between_depth
        left_lower_value = row[column_identifiers_left[i - 1]]
        right_lower_value = row[column_identifiers_right[i - 1]]

        if left_lower_value + self.threshold_between_depth < left_value and not left_del:
            self.to_change.append((int(row_num), column_identifiers_left[i]))
            #print("D",left_value, left_lower_value, ":", left_value)

        if right_lower_value + self.threshold_between_depth < right_value and not right_del:
            self.to_change.append((int(row_num), column_identifiers_right[i]))
            #print("D",right_value, right_lower_value, ":", right_value)

    def process(self, df: pd.DataFrame, station: str):
        logging.debug(f"parsing data for station {station} ...")

        # check if all required fields are available
        all_requested_columns_available = set(
            self.ELEMENT_ORDER_LEFT + self.ELEMENT_ORDER_RIGHT
        ).issubset(df.columns)
        if not all_requested_columns_available:
            logging.debug(
                f"did not found all required columns in {station} to run {self.ps_name}"
            )
            return df

        if df is None:
            return None

        for row_num, row in df.iterrows():
          [self.compare_and_prepare_to_change(self.ELEMENT_ORDER_LEFT, self.ELEMENT_ORDER_RIGHT,row_num, row, i) for i in range(3)]

        # run do change for all to change values
        [
            self.__do_change__(df, row_num, column_name)
            for row_num, column_name in self.to_change
        ]
        self.to_change = []
        # write logs
        self.write_log(station)
        return df
