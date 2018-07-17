import time
import json
import pandas as pd
import math
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from datetime import datetime, timedelta
from pytz import timezone
import datetime######################## MAIN ########################
import pytz
import hashlib
import argparse
import configparser
import subprocess
import numpy as np
from datetime import datetime
from tabulate import tabulate
import os
import sys
print(sys.path)
#sys.path.append('.')
#from ..influx_dataframe_client import Influx_Dataframe_Client
#import Influx_Dataframe_Client
from Influx_Dataframe_Client import Influx_Dataframe_Client

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
    #print(d.strftime('%Y-%m-%dT%H:%M:%SZ'))
    localtz = timezone('US/Pacific')
    d = localtz.localize(d)
    d = d.astimezone(timezone('UTC'))
    return d


def parse_ap(s):
    return s.split('-')

def get_building(s):
    return s[1]

def get_floor(s):
    x=0
    if (len(s[2]) > 3):
        while(len(s[2]) > x and s[2][x].isalpha()):
            if(x == (len(s[2]) -1)):
                return 99
            else:
                x += 1
        return s[2][x]
    else:
        return 1

def get_room(s):
    x=0
    if (len(s[2]) > 3):
        while(len(s[2]) > x and s[2][x].isalpha()):
            if(x == (len(s[2]) -1)):
                return 99
            else:
                x += 1
        x += 1
        return s[2][x:]
    else:
        return s[2]


def post_to_DB(client,json):
    ret = client.write_points(json,batch_size=16384)



def build_json(data, tags, fields, measurement):

    data['measurement'] = measurement

    #data["fields"] = data.iloc[:,2].apply(transform_to_dict, tags=fields)
    data["tags"] = data.apply(transform_to_dict, tags=tags, axis=1)
    data["fields"] = data.apply(transform_to_dict, tags=fields, axis=1)
    #data["tags"] = data.iloc[:,1].apply(transform_to_dict, tags=tags)

    #build a list of dictionaries containing json data to give to client
    #only take relevant columns from dataframe
    json = data[["measurement","time", "tags", "fields"]].to_dict("records")
    print(data.head())
    return json

######################## MAIN ########################

def main(conf_file="/home/werd/lbnl_summer/wifi_data_push/wifi_scraping_config.ini"):
    # read arguments passed at .py file call
    parser = argparse.ArgumentParser()
    parser.add_argument("pickle", help="pickle file")

    args = parser.parse_args()
    pickle_file = args.pickle

    test_client = Influx_Dataframe_Client('/home/werd/lbnl_summer/wifi_data_push/local_server.ini')
    test_client.make_client()

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
    tags = tags.split(',')
    fields = Config.get("wifi_metadata", "fields")
    fields=fields.split(',')



    # get and summarize wifi data
    whole_data = pd.read_pickle(pickle_file)
    data = whole_data.head()
    #get dataframe in right format before creating json
    #puts all of the columns associated with a timestamp together
    data = data.stack()
    data = data.reset_index()

    #change time to correct format and put into UTC from PST

    print(data.head())
    data.iloc[:,0] = data.iloc[:,0].apply(time_transform)

    #rename columns so that the proper keys will appear in the json list
    #containing all the individual measurement, also make sure time is int not float
    data.columns = ['time', 'ap_name', 'AP_count']
    print(data.head())
    #make new column with split up AP name
    data['parse_ap_name'] = data.iloc[:,1].apply(parse_ap)
    #use split AP name to get building, floor and room number
    data['building_number'] = data['parse_ap_name'].apply(get_building)
    data['floor'] = data['parse_ap_name'].apply(get_floor)
    data['room'] = data['parse_ap_name'].apply(get_room)

    print(data.head())
    #remove all of the rows which have NaN
    #optional depending on if your data contains NaN
    data.dropna(inplace=True)

    test_client.write_data(data,tags,fields,measurement,database='new_wifi_data')

    return

if __name__ == "__main__":
    main()
