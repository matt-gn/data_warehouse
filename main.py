""" AMRDC Data Warehouse - This Flask app provides a user interface to query, display, slice,
and download AMRDC AWS data."""
import datetime
import sqlite3
import flask_excel
from flask import Flask, jsonify, request, render_template, url_for

### TODO Query: Implement 'download' links; API hook with JSON response
### TODO Download: Add 'all' and datetime search

app = Flask(__name__)
DB = "static/db/aws.db"


@app.route("/")
def home_page():
    """Fetches the list of available years for AWS and renders the homepage template"""
    with sqlite3.connect(DB) as connection:
        yearlist = sorted(
            year[0]
            for year in connection.execute("SELECT * FROM aws_10min_years").fetchall()
        )
    return render_template("index.html", yearlist=yearlist)


@app.route("/station_list")
def station_list():
    """Queries the database for available stations based on selected years"""
    years = [str(year) for year in request.args.get("year").split(",")]
    with sqlite3.connect(DB) as connection:
        names = sorted(
            connection.execute(
                f"SELECT DISTINCT(name) FROM aws_10min WHERE strftime('%Y', datetime) IN ({','.join('?'*len(years))})",
                years,
            ).fetchall()
        )
    return jsonify(names)


@app.route("/download", methods=["GET"])
def download_data():
    """Collects user input, queries the database, constructs and returns our payload"""
    years = tuple(request.args.get("year").split(","))
    stations = tuple(request.args.get("station").split(","))
    measurements = tuple(request.args.get("meas").split(","))
    extension_type = request.args.get("format")
    data_array = query_db(years, stations, measurements)
    flask_excel.init_excel(app)
    filename = f"AMRDC_AWS_datawarehouse_{datetime.date.today()}.{extension_type}"
    return flask_excel.make_response_from_array(
        data_array, file_type=extension_type, file_name=filename
    )


@app.route("/citation", methods=["GET"])
def generate_citation():
    """Generate an AWS Collection citation based on user input"""
    years = request.args.get("year").split(",")
    yearrange = len(years)
    if yearrange == 0:
        return "Error: Incomplete query"
    elif yearrange == 1:
        return f"Antarctic Meteorological Research and Data Center: Automatic Weather Station quality-controlled observational data, {years[0]}. AMRDC Data Repository, accessed {datetime.date.today()}, https://doi.org/10.48567/1hn2-nw60."
    else:
        return f"Antarctic Meteorological Research and Data Center: Automatic Weather Station quality-controlled observational data. AMRDC Data Repository. Subset used: {years[0]} - {years[-1]}, accessed {datetime.date.today()}, https://doi.org/10.48567/1hn2-nw60."


def query_db(years, names, measurements):
    """Assemble the SELECT statement and query the database"""
    ### Parse input for AWS names and years and format the strings for use in the database query
    namelist = tuple(name.replace("%20", " ") for name in names)
    yearlist = tuple(str(year) for year in years)
    ### Generate SELECT expression and create header for download file
    select = f"SELECT name, datetime, {', '.join(measurements)} FROM aws_10min WHERE name IN ({','.join('?'*len(namelist))}) AND strftime('%Y', datetime) IN ({','.join('?'*len(yearlist))})"
    header = ("name", "datetime") + measurements
    ### Execute query and return
    with sqlite3.connect(DB) as connection:
        data_array = connection.execute(select, namelist + yearlist).fetchall()
    return tuple(header, data_array)


###########################
###     QUERY PAGE      ###
###########################


@app.route("/query", methods=["GET"])
def query():
    """Renders the user input box"""
    query_type = request.args.get("type")
    if not query_type:
        query_type = "all"
    fields = init_fields(query_type)
    return render_template("query.html", query_type=query_type, fields=fields)


@app.route("/results", methods=["POST"])
def query_results():
    """Queries the database and returns the results in the table template"""
    query_type = request.form.get("query_type")
    locations = request.form.getlist("locations") or ["all"]
    if "all" in locations:
        locations = ["all"]
    interval = request.form.get("intervals")
    measurements = request.form.get("measurements")
    grouping = request.form.get("groupings")
    startdate = (
        request.form.get("startyear").rjust(4, "0")
        + request.form.get("startmonth").rjust(2, "0")
        + request.form.get("startday").rjust(2, "0")
    )
    enddate = (
        request.form.get("endyear").rjust(4, "0")
        + request.form.get("endmonth").rjust(2, "0")
        + request.form.get("endday").rjust(2, "0")
    )
    fields = init_fields(
        query_type,
        measurements,
        locations,
        interval,
        grouping,
        startdate,
        enddate,
    )
    select = generate_query(
        query_type,
        measurements,
        locations,
        interval,
        grouping,
        startdate,
        enddate,
    )
    print(select)  ## DEBUG
    with sqlite3.connect(DB) as connection:
        response = connection.execute(select[0], select[1])
        results = response.fetchall()
        columns = tuple(col[0] for col in response.description)
    return render_template(
        "query_results.html",
        query_type=query_type,
        fields=fields,
        results=results,
        columns=columns,
    )


def init_fields(
    query_type="",
    measurement="",
    locations="",
    interval="",
    grouping="",
    startdate="",
    enddate="",
):
    """Initializes the dropdown menus for the user input; also records + returns previous input"""
    with sqlite3.connect(DB) as connection:
        data_locations = sorted(
            x[0] for x in connection.execute("SELECT * FROM aws_10min_names").fetchall()
        )
        yearlist = sorted(
            x[0] for x in connection.execute("SELECT * FROM aws_10min_years").fetchall()
        )
    fields = {
        "query_types": {
            "all": "all datapoints",
            "avg": "average",
            "max": "maximum",
            "min": "minimum",
        },
        "measurements": {
            "temperature": "temperature",
            "wind_speed": "wind speed",
            "pressure": "pressure",
            "humidity": "humidity",
        },
        "data_locations": data_locations,
        "yearlist": yearlist,
        "intervals": {
            "999": "Daily intervals",
            "300": "3 hour intervals",
            "100": "1 hour intervals",
            "10": "10 min intervals",
        },
        "groupings": {
            "year": "grouped by year",
            "month": "grouped by month",
            "day": "grouped by day",
            "name": "for entire record",
        },
        "selected": {
            "query_type": query_type,
            "measurements": measurement,
            "locations": locations,
            "interval": interval,
            "grouping": grouping,
            "startyear": startdate[0:4],
            "startmonth": startdate[4:6],
            "startday": startdate[6:8],
            "endyear": enddate[0:4],
            "endmonth": enddate[4:6],
            "endday": enddate[6:8],
        },
    }
    return fields


def generate_query(
    query_type, measurement, locations, interval, grouping, startdate, enddate
):
    """Generates the SQL SELECT statement based on user input"""
    # ALL DATA, EACH STATION
    if query_type == "all":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, strftime('%H:%M', datetime) as Time, temperature, pressure, wind_speed, wind_direction, humidity, delta_t FROM aws_10min WHERE dateint BETWEEN ? AND ? AND strftime('%H%M', datetime) % ? = 0 AND name IN ({','.join('?'*len(locations))}) LIMIT 2000",
            [int(startdate), int(enddate), interval] + locations,
        ]
    ##  AVG, ALL/SELECTED, YEAR
    elif query_type == "avg" and grouping == "year":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, avg({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  AVG, ALL/SELECTED, MONTH
    elif query_type == "avg" and grouping == "month":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, avg({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y%m', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  AVG, ALL/SELECTED, DAY
    elif query_type == "avg" and grouping == "day":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, avg({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y%m%d', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  AVG, ALL/SELECTED, NAME
    elif query_type == "avg" and grouping == "name":
        select = [
            f"SELECT name, avg({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  MAX/MIN, ALL/SELECTED, YEAR
    elif query_type in ("max", "min") and grouping == "year":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, strftime('%H:%M', datetime) as Time, {query_type}({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  MAX/MIN, ALL/SELECTED, MONTH
    elif query_type in ("max", "min") and grouping == "month":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, strftime('%H:%M', datetime) as Time, {query_type}({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y%m', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  MAX/MIN, ALL/SELECTED, DAY
    elif query_type in ("max", "min") and grouping == "day":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, strftime('%H:%M', datetime) as Time, {query_type}({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name, strftime('%Y%m%d', datetime) LIMIT 2000",
            [int(startdate), int(enddate)] + locations,
        ]
    ##  MAX/MIN, ALL/SELECTED, NAME
    elif query_type in ("max", "min") and grouping == "name":
        select = [
            f"SELECT name, strftime('%Y', datetime) as Year, strftime('%m', datetime) as Month, strftime('%d', datetime) as Day, strftime('%H:%M', datetime) as Time, {query_type}({measurement}) FROM aws_10min WHERE dateint BETWEEN ? AND ? AND ({measurement} != 444) AND name IN ({','.join('?'*len(locations))}) GROUP BY name",
            [int(startdate), int(enddate)] + locations,
        ]
    if locations == ["all"]:
        select[0] = select[0].replace(" AND name IN (?)", "")
        select[1].remove("all")
    return select


###########################
###     MAIN            ###
###########################

if __name__ == "__main__":
    flask_excel.init_excel(app)
    app.run(port=8000, debug=True)
