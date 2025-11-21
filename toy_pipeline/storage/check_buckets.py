

import configparser
from rgw_client import RGWClient
import json
import pandas as pd
from pathlib import Path
import csv
import os
import re
import unicodedata


# config path
path_base="pathbase"
CONFIG_FILE = path_base+'/storage/'+'app.config'

def load_config(file_path):
    
    if not os.path.exists(file_path):
        print(f" config file '{file_path}' not present.")
        return None

    config = configparser.ConfigParser()
    try:
       
        config.read(file_path)
    except Exception as e:
        print(f" Error")
        return None

     
    user_id = config.get('S3_SETUP', 'USER_ID')
    s3_endpoint_url = config.get('S3_SETUP', 'S3_ENDPOINT_URL')
    
    access_key = config.get('CREDENTIALS', 'ACCESS_KEY')
    secret_key = config.get('CREDENTIALS', 'SECRET_KEY')
    
    region = config.get('DEFAULT', 'Region')
    
    #dict
    settings = {
        'USER_ID': user_id,
        'S3_ENDPOINT_URL': s3_endpoint_url,
        'ACCESS_KEY': access_key,
        'SECRET_KEY': secret_key,
        'REGION': region
    }
    
    return settings


settings = load_config(CONFIG_FILE)

# --- CONF---
RGW_ENDPOINT = settings['S3_ENDPOINT_URL']
ACCESS_KEY = settings['ACCESS_KEY']
SECRET_KEY = settings['SECRET_KEY']
USER_ID = settings['USER_ID']

# --- CLIENT ---
rgw = RGWClient(RGW_ENDPOINT, ACCESS_KEY, SECRET_KEY, verify=False)

# --- FUNCTIONS---



bucket_history="bucket_history.json"



def list_buckets():
    

    try:
        buckets = rgw.list_buckets()
    except:
        buckets=[]

    return buckets

buckets=list_buckets()
buckets_fill=[]
if len(buckets)>0:
    for bucket_name in buckets:
        obj=rgw.list_objects(bucket_name)
        if len(obj)==0:
            buckets_fill.append(bucket_name)
            
print(buckets_fill)
file_fill=path_base+'/storage/'+'buckets_fill.json'
file={'buckets':buckets_fill}
with open(file_fill, "w") as f:
    json.dump(file, f, indent=2)


     

