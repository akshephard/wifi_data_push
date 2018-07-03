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
    #print(key)
    dic = {}
    #print(s)
    dic[key] = s
    '''
    for tag in tags:
        dic[tag] = s[tag]
    '''
    return dic

def post_to_DB(client,data,measurement,tags,fields):
    unique_list = []
    output = set()
    #print(data.head())
    #data = data.dropna()
    #data = data.dropna(axis='columns')
    data = data.stack()
    print(data.head())
    #print(list(data.head()))
    data.columns = ['time', 'ap_name','fields']

    print(data.head())
    #print(data.columns)
    data = data.reset_index()
    #print(type(data.iloc(axis=0)[1,0]))
    print(data.dtypes)
    data.iloc[:,0] = data.iloc[:,0].apply(time_transform)
    data.rename(index=str, columns={"level_1": "ap_name", "0": "connected_devices"})
    data.columns = ['time', 'ap_name','connected_devices']
    #data['connected_devices'] = data['connected_devices'].dropna()
    data.dropna(inplace=True)
    data['measurement'] = measurement
    print(data.head())
    data["fields"] = data.iloc[:,2].apply(transform_to_dict, key='connected_devices')
    data["tags"] = data.iloc[:,1].apply(transform_to_dict, key='ap_name')
    print(data.head())
    #json = data[["measurement","time", "tags", "fields"]].head().to_dict("records")
    #json = data.head().to_dict("records")
    print(data.dtypes)
    data['time'] = data['time'].astype(int)
    print(data.dtypes)
    print(data.head())
    json = data[["measurement","time", "tags", "fields"]].to_dict("records")
    ret = client.write_points(json,batch_size=16384)
    #print(json)
'''
    #json = data[['ap_name', 'fields']].to_dict("records")
    #print(json)
    #data["time"] = data.loc['time'].apply(time_transform, time='time')
    #print(data.head())
    #print(tabulate(data, headers='keys', tablefmt='psql'))
    #current_ap_name = data.iloc[[0],[0]].dtypes.index[0]

    for colNum in range(data.shape[1]): #loop through all columns

        #get column name which the name of the current access point
        current_ap_name = data.iloc[[0],[colNum]].dtypes.index[0]
        print(current_ap_name)
        #print(data.head())
        #print(data.stack().head())
        #print(colNum)
        #parse ap name to map to building information
        #parsed_ap_name = current_ap_name.split('-')
        #print(parsed_ap_name)

        pushData = []

        for rowNum in range(data.shape[0]): #loop through all rows

            #get value from data frame a particular row x and column y
            ap_value = float(data.iloc[[rowNum],[colNum]].values)

            #check if value is nan and drop point if it is
            if (math.isnan(ap_value)):
                continue

            #get timestamp as index and convert it to Unix Epoch Pacific Time
            timestamp = data.iloc[[rowNum],[1]].axes[0].tolist()[0]
            d = timestamp.to_pydatetime()
            pushTime = int(time.mktime(d.timetuple()))

            #create formatted json to push to database
            json_dict = {
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
            pushData.append(json_dict)

        ret = client.write_points(pushData)
        if(ret == False):
            print("Failed to push")
            break;
'''
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
