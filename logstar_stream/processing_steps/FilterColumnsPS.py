
import pandas as pd
from typing import Dict
import logging

from processing_steps.ProcessingStep import ProcessingStep

class FilterColumnsPS(ProcessingStep):

    ps_name = "FilterColumnsPS"

    ps_description = """
      Filters out all columns not given via the columns argument

      usage like:
      
      python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm"
      """

    def __init__(self, args: Dict):
        super().__init__(args)

    def process(self, df: pd.DataFrame, station: str):
        columns = self.args['columns'].split(" ")
        logging.debug(f"running {self.ps_name} and only pass through following columns: {columns} ...")
        
        # check if all required fields are available
        all_requested_columns_available = set(columns).issubset(df.columns)
        if not all_requested_columns_available:
            logging.debug(f"did not found all required columns in {station} to run {self.ps_name}")
            return pd.DataFrame()


        df = df[df.columns.intersection(columns, sort=False) ]

        return  df[columns]