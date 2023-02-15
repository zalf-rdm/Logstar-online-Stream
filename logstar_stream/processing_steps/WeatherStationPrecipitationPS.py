from typing import List, Dict
import logging

import pandas as pd

from logstar_stream.processing_steps.ProcessingStep import ProcessingStep


class WeatherStationPrecipitationPS(ProcessingStep):
    ps_name = "WeatherStationPrecipitationPS"

    # value to fill if missmeasurement detected
    ERROR_VALUE = float("NaN")

    # apply to stations
    ALLOWED_STATIONS = [
        "weather_station_01",
        "weather_station_02",
        "weather_station_02_new",
    ]

    # apply to value
    COLUMN_NAME = "precipitation_surface_-200_cm"

    def __init__(self, args: Dict):
        super().__init__(args)

    def process(self, df: pd.DataFrame, station: str, argument: List = None):
        if station not in self.ALLOWED_STATIONS:
            return

        if df is None:
            return

        for i, row in df.iterrows():
            if i == 0 or i == len(df) or row[self.COLUMN_NAME] == 0.0:
                continue

            if (
                row[self.COLUMN_NAME]
                == df.at[i - 1, self.COLUMN_NAME]
                == df.at[i + 1, self.COLUMN_NAME]
            ):
                logging.debug(
                    f"{self.ps_name} | {row['date']} {row['time']}: {row[self.COLUMN_NAME]} -> {self.ERROR_VALUE}"
                )
                changed_object = {
                    "messurement": self.COLUMN_NAME,
                    "date": row["date"],
                    "time": row["time"],
                    "old_value": df.at[i, self.COLUMN_NAME],
                    "new_value": self.ERROR_VALUE,
                }
                df.at[i, self.COLUMN_NAME] = self.ERROR_VALUE
                self.changed.append(changed_object)
        self.write_log(station)
        return df
