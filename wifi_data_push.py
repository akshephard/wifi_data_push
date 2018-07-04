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
from tabulate import tabulate

'''
loop over each columnn in dataframe grab column name first and parse name
add all points as a list of dictionaries of json
i.e.

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
                    },
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

'''
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

def time_transform(timestamp):
    d = timestamp.to_pydatetime()
    pushTime = int(time.mktime(d.timetuple()))
    #print(pushTime)
    return int(pushTime)

def transform_to_dict(s, key):
    dic = {}
    dic[key] = s
    return dic

def post_to_DB(client,json):
    ret = client.write_points(json,batch_size=16384)



def build_json(data, tags, fields, measurement):
    print(data.head())
    print(data.dtypes)

    data['measurement'] = measurement
    print(data.head())
    data["fields"] = data.iloc[:,2].apply(transform_to_dict, key=fields)
    data["tags"] = data.iloc[:,1].apply(transform_to_dict, key=tags)
    print(data.dtypes)
    print(data.head())

    #build a list of dictionaries containing json data to give to client
    #only take relevant columns from dataframe
    json = data[["measurement","time", "tags", "fields"]].head().to_dict("records")
    print(json)

    return json

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

    #get dataframe in right format before creating json
    #puts all of the columns associated with a timestamp together
    data = data.stack()
    data = data.reset_index()

    #change time in correct format
    #time_transform changes from x to Unix Epoch time
    #time can also be in x format, make sure that final datatype is string or int
    data.iloc[:,0] = data.iloc[:,0].apply(time_transform)

    #rename columns so that the proper keys will appear in the json list
    #containing all the individual measurement, also make sure time is int not float
    data.columns = ['time', tags, fields]
    data['time'] = data['time'].astype(int)

    #remove all of the rows which have NaN
    #optional depending on if your data contains NaN
    data.dropna(inplace=True)

    # create client
    client =get_DB_client(host=host,
                          username=username,
                          password=password,
                          database=database,
                          port=port,
                          ssl=False,
                          verify_ssl=True)
    #create json dictionary list to give to the client
    json = build_json(data,tags,fields,measurement)

    #post to db
    ret = post_to_DB(client,json)
    return

if __name__ == "__main__":
    main()
