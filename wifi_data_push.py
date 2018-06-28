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
import configparser
import subprocess
import numpy as np
from datetime import datetime

class Wifi_Push:
    def __init__(self, host='localhost', port=8086, database='wifi',
        username='admin', password='password', measurement='wifi_data', tag='ap_name', field_name='AP_count', pickle_file='default.pkl'):

        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.measurement = measurement
        self.tag = tag
        self.field_name = field_name

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


def post_to_DB(client,data,measurement,tags,fields):
    for x in range(data.shape[0]): #loop through all rows

        #get timestamp as index and convert it to Unix Epoch Pacific Time
        timestamp = data.iloc[[x],[1]].axes[0].tolist()[0]
        d = timestamp.to_pydatetime()
        pushTime = int(time.mktime(d.timetuple()))

        for y in range(data.shape[1]): #loop through all columns
            #get value from data frame a particular row x and column y
            ap_value = float(data.iloc[[x],[y]].values)

            #get column name which the name of the current access point
            current_ap_name = data.iloc[[x],[y]].dtypes.index[0]

            #parse ap name to map to building information
            parsed_ap_name = current_ap_name.split('-')

            #create formatted json to push to database
            pushData = [
                    {
                        "measurement": measurement,
                        "tags": {
                            tags: current_ap_name,
                            "building_number": 90,
                            "floor": 2,
                            "room": 50
                        },
                        "time": pushTime,
                        "fields": {
                            fields: ap_value
                        }
                    }
                ]
            #check to see if value is NaN before pushing
            if (not math.isnan(ap_value)):
                ret = client.write_points(pushData)
                if(ret == False):
                    print("Failed to push")
                    break;

######################## MAIN ########################

def main(conf_file="wifi_scraping_config.ini"):
    obj = Wifi_Push()
    # read arguments passed at .py file call
    parser = argparse.ArgumentParser()
    parser.add_argument("pickle", help="pickle file")

    args = parser.parse_args()
    pickle_file = args.pickle


    # read from config file
    conf_file = conf_file
    Config = configparser.ConfigParser()
    Config.read(conf_file)
    host = Config.get("DB_config", "host")
    username = Config.get("DB_config", "username")
    password = Config.get("DB_config", "password")
    database = Config.get("DB_config", "database")
    protocol = Config.get("DB_config", "protocol")
    measurement = Config.get("DB_config","measurement")
    port = Config.get("DB_config", "port")
    tags = Config.get("wifi_metadata", "tags")
    fields = Config.get("wifi_metadata", "fields")



    # get and summarize wifi data
    data = obj.get_wifi_data(pickle_file)

    # post to db
    client =get_DB_client(host=host,
                          username=username,
                          password=password,
                          database=database,
                          port=port,
                          ssl=True,
                          verify_ssl=True)

    ret = post_to_DB(client,data,measurement,tags,fields)

    return

if __name__ == "__main__":
    main()
