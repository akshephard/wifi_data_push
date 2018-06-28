import time
import json
import pandas as pd
import math
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from datetime import datetime, timedelta
import datetime######################## MAIN ########################
import pytz
import hashlib
import argparse
import configparser
import subprocess
import numpy as np
from datetime import datetime

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

def main(conf_file="wifi_local_config.ini"):
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
    data = pd.read_pickle(pickle_file)

    # post to db
    client =get_DB_client(host=host,
                          username=username,
                          password=password,
                          database=database,
                          port=port,
                          ssl=False,
                          verify_ssl=True)

    ret = post_to_DB(client,data,measurement,tags,fields)

    return

if __name__ == "__main__":
    main()
