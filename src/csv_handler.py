
import csv
import os
import logging


def folder_exists(csv_outfolder):
  return os.path.exists(csv_outfolder)

def write_csv_file(csv_folder, name, dict_request):
  filename = name + ".csv"
  filepath = os.path.join(csv_folder, filename)
  with open(filepath, 'w') as csvfile:
    logging.info("writing csv file: %s ..." % filepath)
    field_names = ["timestamp"] + list(dict_request['header'].values())
    csvwriter = csv.DictWriter(csvfile,fieldnames=field_names)
    csvwriter.writeheader()
    for row in dict_request["data"]:
      row["timestamp"] = row['date'] + " " + row['time']
      del row['date']
      del row['time']
      for k,v in dict_request["header"].items():
        if k == "date" or  k == "time":
          continue
        row[v] = row.pop(k)
      csvwriter.writerow(row)