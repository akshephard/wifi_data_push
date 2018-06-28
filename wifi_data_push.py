import time
import json
import pandas as pd
import math
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
from datetime import datetime

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
print(data.head().reset_index().to_json(orient='records'))

measurement = 'Wifi_AP'
tags = 'ap_name'
fields = 'connected_devices'
#with pd.option_context('display.max_rows', None, 'display.max_columns', 3):
#    print(data)

print(data.head())

host='localhost'
port=8086
database='wifi'
username='admin'
password='password'
client = get_DB_client(host=host,username=username,password=password,database=database,port=port,ssl=True,verify_ssl=True)

ap_value = float(data.iloc[0:1,0:1].values)
print(ap_value)
headers = data.iloc[0:1,0:1].dtypes.index
print(headers[0])
print(data.shape)
print(data.shape[0])
client = get_DB_client(host=host,username=username,password=password,database=database,port=port,ssl=False,verify_ssl=True)
##data.axes[].tolist()

for x in range(data.shape[0]):
    for y in range(data.shape[1]):
        ap_value = float(data.iloc[[x],[y]].values)
        #print(data.iloc[[x],[1]].values)
        #rint(data.iloc[[x],[1]].dtypes.index[0])
        timestamp = data.iloc[[x],[1]].axes[0].tolist()[0]
        #pd.to_timedelta(data.iloc[[x],[1]].axes[0].tolist()[0]).dt.total_seconds().astype(int)
        d = timestamp.to_pydatetime()
        #print(d)
        pushTime = int(time.mktime(d.timetuple()))
        #print(pushTime)
        #print(pd.to_timedelta(data).dt.total_seconds().astype(int))
        #print(type(timestamp))
        #print(timestamp)
        #print("time is " + int(data.iloc[[x],[1]].axes[0].tolist()[0]))
        #print(datetime.fromtimestamp(timestamp))
        pushData =[
                {
                    "measurement": "wifi_data",
                    "tags": {
                        "ap_name": data.iloc[[x],[y]].dtypes.index[0],
                        "building_number": 90,
                        "floor": 2,
                        "room": 50
                    },
                    "time": pushTime,
                    "fields": {
                        "AP_count": ap_value
                    }
                }
            ]
        if (math.isnan(ap_value)):
            print("found nan value")
        else:
            ret = post_to_DB(client,pushData)
            print("we posting")
