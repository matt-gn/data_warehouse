# AMRDC Data Warehouse

View prototype here:
http://ec2-3-16-41-55.us-east-2.compute.amazonaws.com/

# Installation

- Check that your system has Python 3.xx installed: `python --version` (Python 3.7 or greater recommended)
- Get required packages: `pip install -r requirements.txt`
- Navigate to `~/data_warehouse/static/db`
- Run `python database.py` to build database. This will take 10-15 minutes and consume roughly 5GB of hard drive space.
- Navigate to `~/data_warehouse` and run `python main.py`
- Application runs on `http://localhost:8080/` in your web browser.

# Database Schema

Tables
======
- aws_10min           ## All data
- aws_10min_names     ## List of all available stations

Columns
=======
- 0|obs_num|INT
- 1|name|TEXT
- 2|dateint|INT
- 3|datetime|TEXT
- 4|temperature|INT
- 5|pressure|INT
- 6|wind_speed|INT
- 7|wind_direction|INT
- 8|humidity|INT
- 9|delta_t|INT

The `dateint` field is efficient for searching within time periods and is formatted YYYYMMDD

The `datetime` field is useful for display purposes. You can refer to SQLite's datetime formatting functions here - https://www.sqlite.org/lang_datefunc.html.

To see the current list of forbidden SQL keywords (or to edit the list), refer to variable `forbidden` in the `api_sql_query()` function.

matt-gn, 12/1/2022
