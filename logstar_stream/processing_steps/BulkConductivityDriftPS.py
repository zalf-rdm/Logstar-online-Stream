from logstar_stream.processing_steps.ProcessingStep import ProcessingStep
import math
import logging

import pandas as pd

class BulkConductivityDriftPS(ProcessingStep):
    ps_name = "BulkConductivityDriftPS"

    ps_description = "TODO"

    # value to fill if missmeasurement detected
    ERROR_VALUE = float("NaN")

    FORBIDDEN_VALUES = [{"value": 0, "duration": 100}]

    treshold_left_to_right = 50
    threshold_between_depth = 60
    threshold_max_value = 400
    
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
        self.threshold_max_value = float(kwargs['threshold_max_value']) if "threshold_max_value" in kwargs else self.threshold_max_value

        self.to_change = []

    def compare_and_prepare_to_change(self, row, row_num):
        
        for i in range(3):
            left_value = row[self.ELEMENT_ORDER_LEFT[i]]
            right_value = row[self.ELEMENT_ORDER_RIGHT[i]]

            left_del = False
            right_del = False

            if math.isnan(left_value):
                pass
            # compare diff between left and right side. If left or right higher than treshold_left_to_right + (left or right) remove the other
            elif left_value - right_value > self.treshold_left_to_right or left_value > self.threshold_max_value:
                    left_del = True
                    self.to_change.append((int(row_num), self.ELEMENT_ORDER_LEFT[i]))
            
            if math.isnan(right_value):
                pass
            elif right_value - left_value > self.treshold_left_to_right or right_value > self.threshold_max_value:
                    right_del = True
                    self.to_change.append((int(row_num), self.ELEMENT_ORDER_RIGHT[i]))

            # if 30cm depth
            if i == 0: 
              continue


            # check distance between depth and next depth is lower than threshold_between_depth
            left_lower_value = row[self.ELEMENT_ORDER_LEFT[i - 1]]
            right_lower_value = row[self.ELEMENT_ORDER_RIGHT[i - 1]]
            
            # check if nan or none is on left side
            if  None in (left_value, left_lower_value) or math.isnan(left_value) or math.isnan(left_lower_value):
                pass
            
            elif left_lower_value + self.threshold_between_depth < left_value and not left_del:
                self.to_change.append((int(row_num), self.ELEMENT_ORDER_LEFT[i]))
                
            if  None in (right_value, right_lower_value) or math.isnan(right_value) or math.isnan(right_lower_value):
                pass
            
            elif right_lower_value + self.threshold_between_depth < right_value and not right_del:
                self.to_change.append((int(row_num), self.ELEMENT_ORDER_RIGHT[i]))
            

    def process(self, df: pd.DataFrame, station: str):
        """
        Process the given DataFrame for a specific station.

        Args:
            df (pd.DataFrame): The DataFrame to process.
            station (str): The name of the station.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
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
        # iterate over each row of the given data
        for row_num, row in df.iterrows():
          [self.compare_and_prepare_to_change(row, row_num)]

        # run do change for all to change values
        [
            self.__do_change__(df, row_num, column_name)
            for row_num, column_name in self.to_change
        ]
        self.to_change = []
        # write logs
        self.write_log(station)
        return df