
import sys
import os
import importlib
from typing import List, Dict
import logging

import pandas as pd

PS_DIR = "logstar_stream/processing_steps/"
PS_MOD_DIR = PS_DIR.replace("/", ".")

PS_LOGGING_DIR = "logs/"


def load_class(p):
    """
    createing a processing step object of a string like: "PSNAME arg1=value1 arg2=value2"
    checks if the PSNAME is a class name in processing_steps folder.
    The class name must have the same name as the file in the ProcessingStep folder 

    param: p creation string

    return object instance
    """

    class_name = p[0]
    args = {}
    for arg_k_v in p[1:]:
        if "=" in arg_k_v:
            k, v = arg_k_v.split("=")
            args[k] = v
        else:
            logging.error(
                "Processing step args require an assignment operator(=). Like key=value ...")
            sys.exit(1)

    module = PS_MOD_DIR + class_name
    logging.info(
        f"loading {class_name} with args {args} as module: {module} ...")
    m = importlib.import_module(module)
    c = eval("m.{}".format(class_name))
    return c(args=args)


class ProcessingStep(object):

    ps_name = "AbstractLogstarOnlineStreamProcessingStep"
    ps_description = "Abstract Processing Step description, please overwrite when inheriting"
    changed = []

    def __init__(self, args: Dict):
        if "PS_LOGGING_DIR" in args:
            self.PS_LOGGING_DIR = args["PS_LOGGING_DIR"]
        self.changed = []
        self.args = args

    def process(self, df: pd.DataFrame, station: str) -> pd.DataFrame:
        """ processes data and may manipulates it """
        raise NotImplementedError

    def __do_change__(self, df, row_num, column_name):
        """
        function to change given values and add them to the changed list. Which is preparation to write the log with write_log

        :param df: dataframe to edit
        :param row_num: row number the to edit value is in
        :param column_name: column name of the value which is to edit
        """
        row = df.iloc[row_num]
        logging.debug(
            f"{self.ps_name} | {column_name} {row['date']} {row['time']}: {row[column_name]} -> {self.ERROR_VALUE}")
        changed_object = {
            "messurement": column_name,
            "date": row['date'],
            "time": row['time'],
            "old_value": df.at[row_num, column_name],
            "new_value": self.ERROR_VALUE
        }
        self.changed.append(changed_object)
        df.at[row_num, column_name] = self.ERROR_VALUE
        return df

    def write_log(self, station):
        """
        write error log for ProcessingStep

        self.changed: a list of dicts of entries changed like:
          changed = [
            {
              "messurement": "precipitation_surface_-200_cm",
              "date": "2020-01-01",
              "time": "16:45:00",
              "old_value": "5.0",
              "new_value": "nan"
            },
          ]
        """

        if not os.path.exists(PS_LOGGING_DIR):
            logging.warning(
                f"processing step logging folder: {PS_LOGGING_DIR} does not exist, skip logging for {self.ps_name} ...")
            return

        if not self.changed:
            return

        log_filename = self.ps_name + "_" + station + ".log"
        with open(os.path.join(PS_LOGGING_DIR, log_filename), "a+") as f:
            [f.write(f"{d['date']} {d['time']} | {station} -- {d['messurement']}: changed from {d['old_value']} -> {d['new_value']}\n")
             for d in self.changed]
        logging.debug(
            f"finished writing {len(self.changed)} entries into changelog for {log_filename} ...")
