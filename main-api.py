import csv
import configparser

import requests

# create configObject to read configuration from pipeline.conf file
config = configparser.ConfigParser()
config.read("pipeline.conf")
host = config.get("api_config", "host")
port = config.get("api_config", "port")

API_URL = f"http://{host}:{port}"

# print(API_URL)

DATA_FOLDER = "data"
table_list = ["addresses", "events", "order-items", "orders", "products", "promos", "users"]
# table_list = ["addresses", "events"]

for tbl in table_list:
    # print(f"{API_URL}/{tbl}")
    response = requests.get(f"{API_URL}/{tbl}")
    data = response.json()

    with open(f"{DATA_FOLDER}/{tbl}.csv", "w", newline='') as f:
        writer = csv.writer(f)
        header = data[0].keys()
        # print(writer)
        # print(header)
        writer.writerow(header)

        for each in data:
            writer.writerow(each.values())


