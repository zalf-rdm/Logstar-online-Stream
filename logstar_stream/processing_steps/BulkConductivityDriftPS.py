from logstar_stream.processing_steps.ProcessingStep import ProcessingStep
import math
import logging

import pandas as pd


class BulkConductivityDriftPS(ProcessingStep):
    ps_name = "BulkConductivityDriftPS"

    ps_description = "TODO"

    # value to fill if missmeasurement detected
    ERROR_VALUE = pd.NA

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
        self.treshold_left_to_right = (
            float(kwargs["treshold_left_to_right"])
            if "treshold_left_to_right" in kwargs
            else self.treshold_left_to_right
        )
        self.threshold_between_depth = (
            float(kwargs["threshold_between_depth"])
            if "threshold_between_depth" in kwargs
            else self.threshold_between_depth
        )
        self.threshold_max_value = (
            float(kwargs["threshold_max_value"])
            if "threshold_max_value" in kwargs
            else self.threshold_max_value
        )

        self.to_change = []
        # same contents as to_change, as a set, so the depth check can ask
        # whether the layer above has already been marked
        self.marked = set()
        self.no_partner = 0
        self.skipped_depth_check = 0

    def __mark__(self, row_num, column_name):
        """mark a single value for removal, keeping to_change and marked in step"""
        # the index label is kept as handed out by iterrows, not cast to int,
        # so this holds up on any index the frame happens to carry
        key = (row_num, column_name)
        if key in self.marked:
            return
        self.marked.add(key)
        self.to_change.append(key)

    def find_forbidden_runs(self, df):
        """
        Mark values that sit inside a run of a forbidden value, as declared in
        FORBIDDEN_VALUES. A probe stuck at one reading for hours is not
        measuring, but the drift rules cannot see it: a stuck 0 is below every
        threshold, and it drags the layer beneath it down as well.

        Each entry is {"value": v, "duration": n} - a run of at least n
        consecutive records equal to v is removed in full.
        """
        for column_name in self.ELEMENT_ORDER_LEFT + self.ELEMENT_ORDER_RIGHT:
            values = df[column_name]
            for forbidden in self.FORBIDDEN_VALUES:
                is_value = values == forbidden["value"]
                if not is_value.any():
                    continue
                # number each consecutive block, then keep the long ones
                block = (is_value != is_value.shift()).cumsum()
                run_length = is_value.groupby(block).transform("size")
                stuck = is_value & (run_length >= forbidden["duration"])
                for row_num in df.index[stuck]:
                    self.__mark__(row_num, column_name)

    def compare_and_prepare_to_change(self, row, row_num):
        for i in range(3):
            left_value = row[self.ELEMENT_ORDER_LEFT[i]]
            right_value = row[self.ELEMENT_ORDER_RIGHT[i]]

            left_del = False
            right_del = False

            left_missing = left_value is None or pd.isnull(left_value)
            right_missing = right_value is None or pd.isnull(right_value)

            # the left/right comparison needs both probes. Where only one is
            # present the ceiling is all that can be applied, so that reading is
            # screened less thoroughly - count it so the gap is visible.
            if left_missing != right_missing:
                self.no_partner += 1

            # compare diff between left and right side. If left or right higher than treshold_left_to_right + (left or right) remove the other
            if not left_missing:
                if left_value > self.threshold_max_value or (
                    not right_missing
                    and left_value - right_value > self.treshold_left_to_right
                ):
                    left_del = True
                    self.__mark__(row_num, self.ELEMENT_ORDER_LEFT[i])

            if not right_missing:
                if right_value > self.threshold_max_value or (
                    not left_missing
                    and right_value - left_value > self.treshold_left_to_right
                ):
                    right_del = True
                    self.__mark__(row_num, self.ELEMENT_ORDER_RIGHT[i])

            # if 30cm depth
            if i == 0:
                continue

            # check distance between depth and next depth is lower than threshold_between_depth
            left_lower_value = row[self.ELEMENT_ORDER_LEFT[i - 1]]
            right_lower_value = row[self.ELEMENT_ORDER_RIGHT[i - 1]]

            # the layer above is only a valid reference if it survived its own
            # checks. Comparing against a value already marked bad lets one bad
            # reading take the sound one below it down with it.
            left_lower_dropped = (row_num, self.ELEMENT_ORDER_LEFT[i - 1]) in self.marked
            right_lower_dropped = (row_num, self.ELEMENT_ORDER_RIGHT[i - 1]) in self.marked

            # check if nan or none is on left side
            if (
                None in (left_value, left_lower_value)
                or pd.isnull(left_value)
                or pd.isnull(left_lower_value)
                or left_lower_dropped
            ):
                if left_lower_dropped:
                    self.skipped_depth_check += 1

            elif (
                left_lower_value + self.threshold_between_depth < left_value
                and not left_del
            ):
                self.__mark__(row_num, self.ELEMENT_ORDER_LEFT[i])

            if (
                None in (right_value, right_lower_value)
                or pd.isnull(right_value)
                or pd.isnull(right_lower_value)
                or right_lower_dropped
            ):
                if right_lower_dropped:
                    self.skipped_depth_check += 1

            elif (
                right_lower_value + self.threshold_between_depth < right_value
                and not right_del
            ):
                self.__mark__(row_num, self.ELEMENT_ORDER_RIGHT[i])

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

        if df is None:
            return None

        # check if all required fields are available
        all_requested_columns_available = set(
            self.ELEMENT_ORDER_LEFT + self.ELEMENT_ORDER_RIGHT
        ).issubset(df.columns)
        if not all_requested_columns_available:
            # warning, not debug: this skips the station entirely and is
            # otherwise invisible at the default log level
            logging.warning(
                f"did not found all required columns in {station}, skipping {self.ps_name}"
            )
            return df

        self.marked = set()
        self.no_partner = 0
        self.skipped_depth_check = 0

        # stuck-value runs first, so the row checks below can see them
        self.find_forbidden_runs(df)

        # iterate over each row of the given data
        for row_num, row in df.iterrows():
            self.compare_and_prepare_to_change(row, row_num)

        # run do change for all to change values
        for row_num, column_name in self.to_change:
            self.__do_change__(df, row_num, column_name)

        logging.debug(
            f"{self.ps_name} | {station}: removed {len(self.to_change)} values, "
            f"{self.no_partner} readings had no partner to compare against, "
            f"{self.skipped_depth_check} depth checks skipped on an already removed layer"
        )

        self.to_change = []
        self.marked = set()
        # write logs
        self.write_log(station)
        # reset, else the next station's log repeats this station's changes
        self.changed = []
        return df
