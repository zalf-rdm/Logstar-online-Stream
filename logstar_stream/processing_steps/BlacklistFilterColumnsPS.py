import pandas as pd
import logging

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep

class BlacklistFilterColumnsPS(ProcessingStep):
    ps_name = "BlacklistFilterColumnsPS"

    ps_description = """
      Filters out all columns given via the columns argument

      usage like:
      
      python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="battery_voltage signal_strength"
      """

    def __init__(self, kwargs):
        super().__init__(kwargs)

    def process(self, df: pd.DataFrame, station: str):
        """
        Process the given pandas DataFrame by removing specified columns.

        Parameters:
            df (pd.DataFrame): The pandas DataFrame to be processed.
            station (str): The station name.

        Returns:
            pd.DataFrame: The processed pandas DataFrame.
        """
        columns = self.args["columns"].split(" ")
        logging.debug(
            f"running {self.ps_name} and removing following columns: {columns} ..."
        )

        try:
            for column in columns:
                df.drop(column, axis=1, inplace=True)
        except:
            logging.error("Could not filter out column: {}".format(column))
        return df
