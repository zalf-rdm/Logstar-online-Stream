# Logstar API Connector 

API-Docs via http://dokuwiki.weather-station-data.com/doku.php?id=:en:start

## Installation

### Prerequisites
Install system dependencies (example for Ubuntu/Debian):
```bash
sudo apt install unixodbc-dev python3-dev postgresql-server-dev-10
```

### Install Python Package
This project uses `pyproject.toml` for dependency management. Install it using pip:
```bash
pip install .
```
Or if you are using `uv`:
```bash
uv run pip install .
```

### Configuration
Before starting the `logstar-receiver.py`, load settings into your environment. You can create a `load-config.sh` based on `load-config.sh-example`.

**Required:**
- `LOGSTAR_APIKEY` - Your Logstar API key (environment variable)
- Start and end dates - Can be provided via:
  - Command-line: `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`
  - Environment variables: `LOGSTAR_STARTDATE` and `LOGSTAR_ENDDATE`
  - Command-line arguments override environment variables

**Optional Environment Variables:**
- `LOGSTAR_DAYTIME` - Datetime format option (default: 0)
- Database configuration (if not using `-nodb` flag)

Load config into environment (Linux):
```bash
source load-config.sh
```
*Note: Uses specified pyodbc driver. To run against a PostgreSQL database, set driver to "PostgreSQL".*

> **Breaking Change (v2.0):** The `LOGSTAR_STATIONS` environment variable has been removed. Stations are now automatically discovered from the API based on the date range. Use `--filter-stations` to limit which stations to process.

## Docker

To test Logstar-online-Stream, run a local database via Docker.

**PostgreSQL (with PostGIS):**
```bash
docker run --network host -e POSTGRES_USER=postgres -e POSTGRES_PASS=postgres -e POSTGRES_DBNAME=logstar kartoza/postgis
```

**MSSQL Server:**
```bash
docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=MyPassword!' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest
```

**Grafana:**
```bash
docker run --network host grafana/grafana
```

## Start Program

The receiver automatically discovers available stations from the Logstar API based on your date range. It downloads data from `startdate` to `enddate` and stores it in the database.

**Basic Usage (All Stations):**
```bash
python logstar-receiver.py
```

**Filter Specific Stations:**
```bash
# Exact station names
python logstar-receiver.py --filter-stations ws1_l1_rtu_BL tbsl1_00172_BL

# Wildcard patterns
python logstar-receiver.py --filter-stations "*_BL"  # All stations ending with _BL
python logstar-receiver.py --filter-stations "tbs6a_*" "ws1_*"  # Multiple patterns
```

**Console Output (View Data in Terminal):**
```bash
python logstar-receiver.py --console-output --filter-stations ws1_l1_rtu_BL
```

**List Available Stations:**
```bash
python logstar-receiver.py --list-stations
```
This displays all available stations for the configured date range without downloading any data. Useful for discovering station names before filtering.

**Specify Date Range via Command-Line:**
```bash
python logstar-receiver.py --start-date 2025-01-01 --end-date 2025-01-02 --list-stations
```
Command-line date arguments override environment variables.

**Continuous Mode:**
```bash
python logstar-receiver.py --ongoing --interval 20
```

**Database Output:**
```bash
python logstar-receiver.py -db --filter-stations "*_BL"
```
Requires database configuration via environment variables.

**Async & Performance:**
The receiver uses `asyncio` and `aiohttp` to download data from multiple stations in parallel, significantly improving performance.

**CSV Output:**
```bash
python logstar-receiver.py -co data/ --filter-stations ws1_l1_rtu_BL
```
*If writing to CSV files, make sure that the target folder is empty or handle file management, as `logstar-receiver` will append data to existing files.*

**Example with Processing Steps:**
Usage of `BlacklistFilterColumnsPS` to filter specific columns:
```bash
python logstar-receiver.py -m sensor_mapping.json -co data/ -ps BlacklistFilterColumnsPS columns="battery_voltage signal_strength"
```

To find out more about processing steps, lookup the additional [docs](./docs/processings_steps.md).

## Getting Help

To see all available command-line options:
```bash
python logstar-receiver.py --help
```

## Command-Line Options

### Station Selection
- `--filter-stations STATION1 STATION2 ...` - Process only specified stations (supports wildcards: `*`, `?`)
  - Examples: `*_BL`, `tbs6a_*`, `ws1_l1_rtu_BL`
- `--list-stations` - List all available stations and exit (no data download)

### Date Range
- `--start-date YYYY-MM-DD` - Start date (overrides LOGSTAR_STARTDATE env var)
- `--end-date YYYY-MM-DD` - End date (overrides LOGSTAR_ENDDATE env var)

### Output Options
- `-db, --database` - Enable writing data to database (requires database configuration)
- `-co, --csv_outdir PATH` - Write data to CSV files in specified directory
- `--console-output` - Print downloaded data to the console

### Continuous Mode
- `-o, --ongoing` - Run continuously, checking for new data
- `-i, --interval MINUTES` - Sampling interval in minutes (default: 20)

### Processing
- `-m, --sensor_mapping_file PATH` - Path to sensor mapping JSON file
- `-ps, --processing-step STEP [ARGS]` - Add processing steps
- `--rename-datetime-column NAME` - Rename the datetime column (only works with LOGSTAR_DAYTIME=0)

### Database Options
- `-dbtp, --db_table_prefix PREFIX` - Prefix for database tables
- `-dbs, --db_schema SCHEMA` - Database schema (default: public)

### Logging
- `-v, --verbose` - Enable verbose logging
- `-l, --log FILE` - Redirect logs to a file

## Station Discovery

Stations are automatically discovered from the Logstar API based on your configured date range. The system queries both the start and end dates to ensure all stations with data in the time range are included.

**How it works:**
1. Queries the API for available stations on the start date
2. Queries the API for available stations on the end date
3. Merges the results to get a comprehensive list
4. Optionally filters the list if `--filter-stations` is provided

This ensures you always have access to the latest stations without manual configuration.