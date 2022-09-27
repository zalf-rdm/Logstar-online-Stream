# Processing steps

Processing in steps in Logstar-stream are meant to run between the download and the file write to csv or database. A processing step can be a data quality assurance function, an alerting or a filter to remove sensor errors.

The processing-step-manager.py can list you all available processing steps. like:

```
python processing-step-manager.py -l
```


## Writing your own processing step
Further writing a processing step on your own is quite simple. To do so the new PS must follow some guidelines:
1. put the new PS inside of the proessing_steps folder
2. inherit from the ProcessingStep class (processing_steps/ProcessingStep.py)
    * implement the process function: ```def process(self, df: pd.DataFrame, station: str):``` . This function gets a dataframe containing data and a stationname the data belongs to
3. Further best practice is to use the write_log function to document which values got changed.

### minimal example PS

```
# this PS filters out columns not part of the columns aruments passed to this PS.

import pandas as pd
from typing import Dict
import logging

from processing_steps.ProcessingStep import ProcessingStep

# class inherit from ProcessingStep
class FilterColumnsPS(ProcessingStep):

    # ps_name variable used in several contexts pls overwrite
    ps_name = "FilterColumnsPS"

    # ps_description used in several contexts pls overwrite
    ps_description = """
      Filters out all columns not given via the columns argument

      usage like:
      
      python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm"
      """

    # init method stores the given args in super class function
    def __init__(self, args: Dict):
        super().__init__(args)

    # main method, this will be called for every station. If running in continous mode this gets called each time the download is finished
    def process(self, df: pd.DataFrame, station: str):
        # get columns from args, may fail if argument is not inserted correct
        columns = self.args['columns'].split(" ")
        logging.debug(f"runnging {self.ps_name} and only pass through following columns: {columns} ...")
        
        # check if all required fields are available
        all_requested_columns_available = set(columns).issubset(df.columns)
        if not all_requested_columns_available:
            logging.debug(f"did not found all required columns in {station} to run {self.ps_name}")
            return pd.DataFrame() # return empty dataframe

        df = df[df.columns.intersection(columns, sort=False) ]

        return  df[columns]
```

## Usage of PS

To use one or multiple PS the logstar argument -ps can be used. Logstar allows the user to chain multiple PS after each other. They will be executed in order appearing in the command call. e.g.:

```
# this will start logstar-receiver 
# * with a sensor-mapping: -m sensor_mapping.json
# * with no database output: -nodb
# * with filtering columns to only allow "time date bulk_conductivity_right_30cm":  -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm"
python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm"
```

To chain multiple PS just add another `-ps` argument to the cmd call like:
```
# adds BulkConductivityDriftPS with no aruments to be executed after the FilterColumnsPS
python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date bulk_conductivity_right_30cm" -ps BulkConductivityDriftPS
```
