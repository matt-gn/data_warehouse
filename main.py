""" AMRDC Data Warehouse - This Flask app provides a user interface to query, display, slice,
and download AMRDC AWS data."""
import datetime
import sqlite3
import flask_excel
from flask import Flask, jsonify, request, render_template, url_for

application = Flask(__name__)
DB = "static/db/aws.db"

###########################
###     QUERY PAGE      ###
###########################


@application.route("/", methods=["GET"])
def query():
    """Renders the user input box"""
    query_type = request.args.get("type")
    if not query_type:
        query_type = "all"
    fields = init_fields(query_type)
    return render_template("query.html", query_type=query_type, fields=fields)


@application.route("/results", methods=["POST"])
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
    if request.form.get("submit") == "display":
        return render_template(
            "query_results.html",
            query_type=query_type,
            fields=fields,
            results=results,
            columns=columns,
        )
    if request.form.get("submit") == "download":
        flask_excel.init_excel(application)
        data_array = tuple(columns) + tuple(results)
        ## MAKE CITATION
        if startdate[0:4] == enddate[0:4]:
            citation = f"Antarctic Meteorological Research and Data Center: Automatic Weather Station quality-controlled observational data, {startdate[0:4]}. AMRDC Data Repository. Accessed {datetime.date.today()}, doi 10.48567/1hn2-nw60"
        else:
            citation = f"Antarctic Meteorological Research and Data Center: Automatic Weather Station quality-controlled observational data. AMRDC Data Repository. Subset years {startdate[0:4]} - {enddate[0:4]}. Accessed {datetime.date.today()}, doi 10.48567/1hn2-nw60"
        return flask_excel.make_response_from_array(
            data_array, file_type="csv", file_name=f"{citation}.csv"
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
###     API HOOK        ###
###########################


@application.route("/api/get", methods=["GET"])
def api_sql_query():
    ## Get query from address + verify
    select = request.args.get("query")
    ## Primitive SQL Injection prevention right here:
    query_terms = [word.upper() for word in select.replace(";", "").replace("%20", " ").split()]
    forbidden = [
        "DROP",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "ADD",
        "ALTER",
        "CREATE",
    ]
    if (set(query_terms) & set(forbidden)) or (query_terms[0] != "SELECT"):
        return jsonify("Bad select statement. No data returned. ")
    ## Query DB and return results as JSON
    with sqlite3.connect(DB) as connection:
        data_array = connection.execute(select).fetchall()
    return jsonify(data_array)


###########################
###     MAIN            ###
###########################

if __name__ == "__main__":
    application.run(host='0.0.0.0', port='8080')
