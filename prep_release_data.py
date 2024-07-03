import os
import pandas as pd
import numpy as np

# to merge the data by datetime its best to predefine the datetime elements in join them late with outer join
# time starting
start_datetime = pd.to_datetime("2020-09-01 00:00:00")
# time ending
end_datetime = pd.to_datetime("2022-12-31 23:40:00")
# generate datetime_column
# all files inside of data/ folder get merged against the range of dates between start and end.
datetime_column = pd.date_range(start=start_datetime, end=end_datetime, freq="20T")


output_folder = "2020-2022"
input_folder = "data/"
GROUPS = {
    "water_content": pd.DataFrame(),
    # "soil_temperature": pd.DataFrame(),
    # "pore_water_conductivity": pd.DataFrame(),
    # "bulk_conductivity": pd.DataFrame(),
    # "permittivity": pd.DataFrame(),
}

for group, _ in GROUPS.items():
    g_df = pd.DataFrame(
        columns=["dateTime", "patch_ID", "parameter", "depth", "sensor_type"]
    )
    # g_df["dateTime"] = pd.to_datetime(g_df["dateTime"])
    # g_df.set_index(f"dateTime")
    GROUPS[group] = g_df

# files to ignore
file_blacklist = [
    "weather_station_02.csv",
    "weather_station_01.csv",
    "weather_station_02.2.csv",
]

# This is the main script merging the data together
file_list = os.listdir(input_folder)


def seperate_file(GROUPS, filepath):
    station = os.path.splitext(f)[0]
    df = pd.read_csv(file_path)
    for group, g_df in GROUPS.items():
        print(station, group)
        # filter out columns related to current group into filtered_df and rename columns
        group_columns = list(df.filter(like=group).columns)
        for group_column in group_columns:
            print(group_column)
            columns = ["dateTime", group_column]
            filtered_df = df.filter(items=columns)
            filtered_df.rename(columns={group_column: "parameter"}, inplace=True)
            depth = group_columns[0][len(group) + 1 :].split("_")[1]
            filtered_df["depth"] = depth
            if "mobil" in station:
                filtered_df["sensor_type"] = "mobile"
                filtered_df["patch_ID"] = (
                    station.split("_")[0] + "_" + station.split("_")[1]
                )
            else:
                filtered_df["sensor_type"] = "permanent"
                filtered_df["patch_ID"] = station
            GROUPS[group] = pd.concat([GROUPS[group], filtered_df], axis=0)


for f in file_list:
    if not f.endswith(".csv") or f in file_blacklist:
        print(f"skipping {f}")
        continue
    file_path = os.path.join(input_folder, f)
    seperate_file(GROUPS, file_path)

GROUPS["water_content"]
