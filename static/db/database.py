import sqlite3, urllib.request, json, ckanapi

DATABASE_NAME = "aws.db"
SERVER_URL = "https://amrdcdata.ssec.wisc.edu/"
API_KEY = "28933df5-45b8-41e0-a873-1e29ebc4aca5"


def main():
    ### Search for AWS QC data on repository and add dataset URLs to record_list list
    with urllib.request.urlopen(
        'https://amrdcdata.ssec.wisc.edu/api/action/package_search?q=title:"quality-controlled+observational+data"&rows=1000'
    ) as response:
        record_list = json.loads(response.read())
    # record_count = record_list['result']['count']         ### NOTE: If record_count > 1000, must slice + return multiple lists (Update 11/16/2022: 817 records)
    data_files = []
    print(
        "Fetching 10min records from repository..."
    )  ### Extract station name and individual data file URLs from every record in record_list and add to data_files list
    for record in record_list["result"]["results"]:
        title = record["title"].split()
        name = []
        for word in title:
            if word == "Automatic":
                break
            name.append(word)
        name_formatted = " ".join(name)
        for item in record["resources"]:
            if "q10" in item["name"]:
                data_files.append([name_formatted, item["url"]])
    print("Creating database...")  ### Build database
    with sqlite3.connect(DATABASE_NAME) as database:
        database.execute(
            "CREATE TABLE aws_10min (obs_num INT PRIMARY KEY, name TEXT, datetime TEXT, temperature INT, pressure INT, wind_speed INT, wind_direction INT, humidity INT, delta_t INT)"
        )
        obs_num = 0
        print(f"Database '{DATABASE_NAME}' created")
        print(
            "Extracting resources"
        )  ### Extract data from every resource in data_files and add to database
        extracted = 0
        for resource in data_files:
            station = resource[0]
            target = resource[1]
            with urllib.request.urlopen(target) as data:
                for line in range(2):
                    next(data)
                for line in data:
                    decoded_line = line.decode("utf-8")
                    row = decoded_line.split()
                    datetime = f"{row[0]}-{row[2].rjust(2, '0')}-{row[3].rjust(2, '0')} {row[4][0:2]}:{row[4][2:]}"
                    database.execute(
                        "INSERT INTO aws_10min VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            obs_num,
                            station,
                            datetime,
                            row[5],
                            row[6],
                            row[7],
                            row[8],
                            row[9],
                            row[10],
                        ),
                    )
                    obs_num += 1
                extracted += 1
        database.execute(
            "CREATE TABLE aws_10min_names AS SELECT DISTINCT(name) FROM aws_10min"
        )
        database.execute(
            "CREATE TABLE aws_10min_years AS SELECT DISTINCT(strftime('%Y', datetime)) FROM aws_10min"
        )
        database.execute("CREATE INDEX aws_10min_index_name ON aws_10min (name)")
        database.execute(
            "CREATE INDEX aws_10min_index_namedate ON aws_10min (name, strftime('%Y%m', datetime))"
        )
        database.commit()
        print(f"Finished with 10 min data. Extracted {extracted} data files.")
    print("Done.")


## TODO: Get format of Reader files and re-do the data & db sections.
def reader():
    repository = ckanapi.RemoteCKAN(SERVER_URL, apikey=API_KEY)
    packages = repository.action.package_search(q="aws reader", rows=1000)
    packages2 = repository.action.package_search(q="aws reader", rows=1000, start=1000)
    data_files = []

    print(
        "Fetching Reader records from repository..."
    )  ### Extract station name and individual data file URLs from every record in record_list and add to data_files list
    for record in packages["results"]:
        title = record["title"].split()
        name = []
        for word in title:
            if word == "Automatic":
                break
            name.append(word)
        name_formatted = " ".join(name)
        for item in record["resources"]:
            if "q10" in item["name"]:
                data_files.append([name_formatted, item["url"]])
    for record in packages2["results"]:
        title = record["title"].split()
        name = []
        for word in title:
            if word == "Automatic":
                break
            name.append(word)
        name_formatted = " ".join(name)
        for item in record["resources"]:
            if "q10" in item["name"]:
                data_files.append([name_formatted, item["url"]])

    print("Connecting to database....")
    database = sqlite3.connect(DATABASE_NAME)
    database.execute(
        "CREATE TABLE aws_3hr_data (obs_num INT PRIMARY KEY, name TEXT, datetime TEXT, temperature INT, pressure INT, wind_speed INT, wind_direction INT, humidity INT, delta_t INT)"
    )

    obs_num = 0
    print(
        "Extracting resources"
    )  ### Extract data from every resource in data_files and add to database
    extracted = 0
    for resource in data_files:
        station = resource[0]
        target = resource[1]
        data = urllib.request.urlopen(target)
        for line in range(2):
            next(data)
        for line in data:
            decoded_line = line.decode("utf-8")
            row = decoded_line.split()
            database.execute(
                "INSERT INTO aws_3hr_data VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}')".format(
                    obs_num,
                    station,
                    row[0],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                )
            )
            obs_num = obs_num + 1
        extracted = extracted + 1

    database.commit()
    database.close()
    print("Finished.")


if __name__ == "__main__":
    main()
