import time
import json
import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from datetime import datetime, timedelta
import datetime
import pytz
import hashlib
import argparse
#import ConfigParser
import subprocess
import numpy as np

class Wifi_Push:
    def __init__(self, host='localhost', port=8086, database='wifi',
        username='admin', password='password'):

        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def get_wifi_data(self, file_name):

        result = pd.read_pickle(file_name)
        return result

    def push_data_cloud_db(self, data, measurement='Wifi_Test'):

        print("Pushing data to cloud db...")

        client = DataFrameClient(host=self.host, port=self.port, database=self.database,
                                username=self.username, password=self.password)

        data = data.replace(['inf', '-inf'], np.nan)
        for c in data.columns:
            result = client.write_points(data[[c]].dropna(), 'measurement')
        #result = client.write_points(data, measurement)
        if not result:
            print("Error: Could not push data to cloud db")
            return False
        else:
            print("Successfully pushed data to cloud db!")
            return True

def get_DB_client(host,
                  username,
                  password,
                  database,
                  port=8086,
                  ssl=True,
                  verify_ssl=True):

    client = InfluxDBClient(host=host, port=port, username=username, password=password, database=database,
                            ssl=ssl, verify_ssl=verify_ssl)
    return client

def transform_to_dict(s, tags):

    dic = {}
    for tag in tags:
        dic[tag] = s[tag]

    return dic

# create json body for influxDB push
def build_json(data, tags, fields, measurement):

    data = data.reset_index()
    data["tags"] = data.apply(transform_to_dict, tags=tags, axis=1)
    data["fields"] = data.apply(transform_to_dict, tags=fields, axis=1)
    data["time"] = get_current_time_utc()
    data["measurement"] = measurement
    json = data[["measurement","time", "tags", "fields"]].to_dict("records")

    return json

# post to db
def post_to_DB(client,json):

    ret = client.write_points(json)

    return ret

obj = Wifi_Push()

data = obj.get_wifi_data('201603.pkl')

measurement = 'Wifi_AP'
tags = 'ap_name'
fields = 'connected_devices'
#with pd.option_context('display.max_rows', None, 'display.max_columns', 3):
#    print(data)

print(data.head())
json = build_json(data, tags, fields, measurement)
host='localhost'
port=8086
database='wifi'
username='admin'
password='password'
client = get_DB_client(host=host,username=username,database=database,port=port,ssl=True,verify_ssl=True)
ret = post_to_DB(client,json)

'''
if data.empty:
    exit()
else:
    counter = 0

    while (counter < 5):
        success = obj.push_data_cloud_db(data, measurement=measurement)
        print("We get here")
        if success:
            break
        else:
            counter += 1
            time.sleep(60)
        if (counter == 5):
            print("Error: Try again later.")
'''
