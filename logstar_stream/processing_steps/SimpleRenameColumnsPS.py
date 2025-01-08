import pandas as pd
import logging
import sys

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class SimpleRenameColumnsPS(ProcessingStep):
    ps_name = "SimpleRenameColumnsPS"

    ps_description = """
      Filters out all columns not given via the columns argument

      usage like:
      
      python logstar-receiver.py -m sensor_mapping.json -nodb -ps SimpleRenameColumnsPS columns="time date bulk_conductivity_right_30cm"
      """

    def __init__(self, kwargs):
        """
        Initializes the SimpleRenameColumnsPS with the given arguments.

        Args:
            kwargs (Dict): The arguments given to this ProcessingStep. Must contain
                the keys "equal", "columns", and "seperator".

        Raises:
            ValueError: If the required arguments are not given.
        """
        super().__init__(kwargs)

        if "equal" not in kwargs or "columns" not in kwargs or "seperator" not in kwargs:
            logging.error("columns or seperator missing in {}".format(self.ps_name))
            logging.error("use like: python logstar-receiver.py  -nodb -co data/ -ps SimpleRenameColumnsPS columns=\"Luftfeuchte - \%rF:Luftfeuchte; \" seperator=\";Lufttemp1 - GradC:Lufttemp1\" equal=\":\"")
            sys.exit(1)
        self.equal = str(kwargs["equal"])
        self.columns = str(kwargs["columns"])
        self.seperator = str(kwargs["seperator"])


    def process(self, df: pd.DataFrame, station: str):
        """
        Process the given DataFrame for a specific station.

        Args:
            df (pd.DataFrame): The DataFrame to be processed.
            station (str): The name of the station.

        Returns:
            pd.DataFrame: The processed DataFrame.
        """
        logging.debug(
            f"running {self.ps_name} and renamning following columns: {self.columns} using seperator: {self.seperator} and equal sign: {self.equal} ..."
        )
        columns = self.columns.split(self.seperator)
        map = {}
        for s in columns:
            try:
              k,v = s.split(self.equal)
              map[k] = v
            except:
              logging.error("could not parse column: {}".format(s))
        df.rename(columns=map,inplace=True)

        # logging.debug(
        #     f"running {self.ps_name} and only pass through following columns: {columns} ..."
        # )

        # # check if all required fields are available
        # all_requested_columns_available = set(columnsself.s).issubset(df.columns)
        # if not all_requested_columns_available:
        #     logging.debug(
        #         f"did not found all required columns in {station} to run {self.ps_name}"
        #     )
        #     return pd.DataFrame()

        # df = df[df.columns.intersection(columns, sort=False)]

        return df
