# Logstar API Connector 

API-Docs via http://dokuwiki.weather-station-data.com/doku.php?id=:en:start

## Installation

install psql-dev
```bash
sudo apt install unixodbc-dev python3-dev postgresql-server-dev-10
```

install pip requirments
```bash
pip install -r requirments.txt
```

before starting the logstar-receiver.py. Load settings into environment. This can be done creating a load-config.sh based on the load-config.sh-example. Load config into environment(Linux) via:
```bash
source load-config.sh
```
Uses specified pyodbc driver. To run against pqsl-database set driver to "PostgreSQL"

## Docker

To test Logstar-online_Stream run a local database via docker

to run a Postgresql-database container:
```bash
docker run --network host -e POSTGRES_USER=postgres -e POSTGRES_PASS=postgres -e POSTGRES_DBNAME=logstar  kartoza/postgis
```

or to run a mssql-database container
```bash
docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=MyPassword!' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest
```

Run Grafana Container
```
docker run --network host grafana/grafana
```

## Start Program

Run programm in download mode. Downloads data in from startdate to enddate and stores it in the database
```bash
python logstar-receiver.py
```

Example usage of processing step FilterColumnsPS with a number of columns to filter:
```bash
python logstar-receiver.py -m sensor_mapping.json -nodb -ps FilterColumnsPS columns="time date water_content_right_30_cm water_content_right_60_cm water_content_right_90_cm water_content_left_30_cm water_content_left_60_cm water_content_left_90_cm" -co data/
```

To find out more about processing steps lookup the additional [docs](./docs/processings_steps.md). 