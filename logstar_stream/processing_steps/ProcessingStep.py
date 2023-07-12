import sys
import os
import importlib
from typing import Dict
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
                "Processing step args require an assignment operator(=). Like key=value ..."
            )
            sys.exit(1)

    module = PS_MOD_DIR + class_name
    logging.info(f"loading {class_name} with args {args} as module: {module} ...")
    m = importlib.import_module(module)
    c = eval("m.{}".format(class_name))
    return c(kwargs=args)


class ProcessingStep(object):
    ps_name = "AbstractLogstarOnlineStreamProcessingStep"
    ps_description = (
        "Abstract Processing Step description, please overwrite when inheriting"
    )
    changed = []

    def __init__(self, kwargs):
        if "PS_LOGGING_DIR" in kwargs:
            self.PS_LOGGING_DIR = kwargs["PS_LOGGING_DIR"]
        self.changed = []
        self.args = kwargs

    def process(self, df: pd.DataFrame, station: str) -> pd.DataFrame:
        """processes data and may manipulates it"""
        raise NotImplementedError

    def __do_change__(self, df, row_num, column_name):
        """
        function to change given values and add them to the changed list. Which is preparation to write the log with write_log

        :param df: dataframe to edit
        :param row_num: row number the to edit value is in
        :param column_name: column name of the value which is to edit
        """
        row = df.iloc[row_num]
        # depends on config.dateTime if 1: „date“: „2020-04-01“, „time“: „00:00:00“
        if "date" in row and "time" in row:
            logging.debug(
                f"{self.ps_name} | {column_name} {row['date']} {row['time']}: {row[column_name]} -> {self.ERROR_VALUE}"
            )
            changed_object = {
                "messurement": column_name,
                "date": row["date"],
                "time": row["time"],
                "old_value": df.at[row_num, column_name],
                "new_value": self.ERROR_VALUE,
            }
        # if 0 (default): „dateTime“: „2020-04-01 00:00:00“
        elif "dateTime" in row:
            changed_object = {
                "messurement": column_name,
                "dateTime": row["dateTime"],
                "old_value": df.at[row_num, column_name],
                "new_value": self.ERROR_VALUE,
            }
            logging.debug(
                f"{self.ps_name} | {column_name} {row['dateTime']}: {row[column_name]} -> {self.ERROR_VALUE}"
            )
        else:
            changed_object = {
                "messurement": column_name,
                "old_value": df.at[row_num, column_name],
                "new_value": self.ERROR_VALUE,
            }
            logging.debug(
                f"{self.ps_name} | {column_name}: {row[column_name]} -> {self.ERROR_VALUE}"
            )

        self.changed.append(changed_object)
        df.at[row_num, column_name] = self.ERROR_VALUE
        return df

    def write_log(self, station) -> None:
        """
        Writes a log entry for the given station.

        Parameters:
            station (str): The name of the station.

        Returns:
            None
        """

        if not os.path.exists(PS_LOGGING_DIR):
            logging.warning(
                f"processing step logging folder: {PS_LOGGING_DIR} does not exist, skip logging for {self.ps_name} ..."
            )
            return

        if not self.changed:
            return

        log_filename = self.ps_name + "_" + station + ".log"

        with open(os.path.join(PS_LOGGING_DIR, log_filename), "a+") as f:
            for d in self.changed:
                # depends on config.dateTime
                if "date" in d and "time" in d:
                    log_string = f"{d['date']} {d['time']} | {station} -- {d['messurement']}: changed from {d['old_value']} -> {d['new_value']}\n"
                elif "dateTime" in d:
                    log_string = f"{d['dateTime']} | {station} -- {d['messurement']}: changed from {d['old_value']} -> {d['new_value']}\n"
                else:
                    log_string = f" ??? | {station} -- {d['messurement']}: changed from {d['old_value']} -> {d['new_value']}\n"

                f.write(log_string)

        logging.debug(
            f"finished writing {len(self.changed)} entries into changelog for {log_filename} ..."
        )
