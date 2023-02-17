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

    threshold = 50

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
        self.threshold = float(kwargs['threshold']) if "threshold" in kwargs else self.threshold
        self.to_change = []

    def __check_wc_for_drift__(self, l):
        if self.ELEMENT_ORDER_LEFT[0] - self.ELEMENT_ORDER_LEFT[1] >= self.threshold:
            pass
        elif self.ELEMENT_ORDER_LEFT[1] - self.ELEMENT_ORDER_LEFT[2] >= self.threshold:
            pass

    def compare_and_prepare_to_change(self, column_identifier_left, column_identifier_right, row_num, row):
        left_value = row[column_identifier_left]
        right_value = row[column_identifier_right]
        
        if None in (left_value, right_value) or math.isnan(left_value) or math.isnan(right_value):
            return

        if left_value - right_value > self.threshold:
            self.to_change.append((int(row_num), column_identifier_left))

        elif right_value - left_value > self.threshold:
            self.to_change.append((int(row_num), column_identifier_right))


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
          [self.compare_and_prepare_to_change(self.ELEMENT_ORDER_LEFT[i], self.ELEMENT_ORDER_RIGHT[i],row_num, row) for i in range(3)]

        #     # iter over left and right
        #     for column_lr in [self.ELEMENT_ORDER_LEFT, self.ELEMENT_ORDER_RIGHT]:
        #         # iter through each entry of ELEMENT_ORDER_RIGHT | ELEMENT_ORDER_LEFT and compare each entry with the
        #         # other two to check if one of them is threshold units above the others, if so add them to to_change list
        #         [
        #             self.compare_and_prepare_to_change(column_lr, row_num, row, i)
        #             for i in range(len(column_lr))
        #         ]

        # run do change for all to change values
        [
            self.__do_change__(df, row_num, column_name)
            for row_num, column_name in self.to_change
        ]
        self.to_change = []
        # write logs
        self.write_log(station)
        return df
