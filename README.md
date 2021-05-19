# Logstar API Connector 

API-Docs via http://dokuwiki.weather-station-data.com/doku.php?id=:en:start

## Installation

install psql-dev
```bash
sudo apt install postgresql-server-dev-10
```

install pip requirments
```bash
pip install -r requirments.txt
```

## Docker

Run Postgresql Container
```bash
docker run --network host -e POSTGRES_USER=postgres -e POSTGRES_PASS=postgres -e POSTGRES_DBNAME=logstar  kartoza/postgis
```

Run Grafana Container
```
docker run --network host grafana/grafana
```