# Changelog

All notable changes to the Logstar Stream project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2025-12-10

### Added
- **API-based Station Discovery**: Stations are now automatically fetched from the Logstar API based on the configured date range
  - Queries both start and end dates to capture all stations with data in the time range
  - Returns a comprehensive, sorted list of available stations
  - Eliminates need for manual station configuration
- **Station Filtering**: New `--filter-stations` command-line argument to selectively process specific stations
  - Accepts space-separated list of station names or wildcard patterns
  - Supports glob-style wildcards: `*` (matches any characters), `?` (matches single character)
  - Warns when patterns don't match any stations
  - Examples: 
    - Exact names: `--filter-stations ws1_l1_rtu_BL tbsl1_00172_BL`
    - Wildcards: `--filter-stations "*_BL"` (all stations ending with _BL)
    - Multiple patterns: `--filter-stations "tbs6a_*" "ws1_*"`
- **Station List Display**: New `--list-stations` flag to display available stations without downloading data
  - Shows all stations in a formatted numbered list
  - Works with `--filter-stations` to show only filtered results
  - Useful for discovering station names before processing
  - No database connection required
  - Example: `python logstar-receiver.py --list-stations`
- **Console Output**: New `--console-output` flag to display downloaded data directly in the terminal
  - Formatted output with station headers and separators
  - Useful for quick data inspection without database or CSV files
- **Date Range Command-Line Parameters**: New `--start-date` and `--end-date` arguments
  - Override environment variables when provided
  - Format: `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`
  - Validation ensures dates are always provided (via CLI or env vars)
  - Example: `python logstar-receiver.py --start-date 2025-01-01 --end-date 2025-01-02`
- **Enhanced Logging**: Improved logging to show:
  - Number of stations discovered from API
  - Filtering results (stations selected vs. available)
  - Station names being processed
  - API query progress

### Changed
- **Asynchronous Station Discovery**: Station fetching is now part of the async workflow
- **Improved Error Handling**: Better error messages for API failures and missing stations
- **Database Writing is Now Opt-in**: Changed from `-nodb` (opt-out) to `-db` (opt-in)
  - Database writing now requires explicit `-db` or `--database` flag
  - Matches the pattern of CSV output (`-co`) for consistency
  - No database connection attempted unless explicitly requested

### Removed
- **BREAKING**: `LOGSTAR_STATIONS` environment variable is no longer used or required
  - Migration: Use `--filter-stations` CLI argument instead to limit which stations to process
  - All stations in the date range are now discovered automatically
- **BREAKING**: `-nodb` / `--disable_database` flag removed
  - Migration: Database writing is now opt-in, use `-db` or `--database` to enable it
  - By default, no database connection is attempted

### Technical Details
- Added `get_available_stations()` method to `LogstarClient` class
- API endpoint used: `https://logstar-online.de/api/{apikey}/all-stations/{date}/0`
- Station discovery queries both start and end dates for comprehensive coverage
- Currently discovers 100+ stations for typical date ranges

## [1.x.x] - Previous Versions

### Features
- Asynchronous data download using `asyncio` and `aiohttp`
- PostgreSQL and MSSQL Server database support
- CSV output support
- Sensor mapping and column renaming
- Processing steps framework
- Continuous/ongoing mode for real-time data collection
- Docker support for testing
- Comprehensive test suite

---

For detailed usage examples and migration guides, see the [README.md](README.md).
