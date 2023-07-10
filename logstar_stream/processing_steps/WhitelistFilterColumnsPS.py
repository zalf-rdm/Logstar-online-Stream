import pandas as pd
import logging

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class WhitelistFilterColumnsPS(ProcessingStep):
    ps_name = "WhitelistFilterColumnsPS"

    ps_description = """
      Filters out all columns not given via the columns argument

      usage like:
      
      python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm"
      """

    def __init__(self, kwargs):
        super().__init__(kwargs)

    def process(self, df: pd.DataFrame, station: str):
        """
        Process the given DataFrame for a specific station.

        Args:
            df (pd.DataFrame): The DataFrame to be processed.
            station (str): The name of the station.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        columns = self.args["columns"].split(" ")
        logging.debug(
            f"running {self.ps_name} and only pass through following columns: {columns} ..."
        )

        # check if all required fields are available
        all_requested_columns_available = set(columns).issubset(df.columns)
        if not all_requested_columns_available:
            logging.debug(
                f"did not found all required columns in {station} to run {self.ps_name}"
            )
            return pd.DataFrame()

        df = df[df.columns.intersection(columns, sort=False)]

        return df[columns]


