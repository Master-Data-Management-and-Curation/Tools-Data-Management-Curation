

import configparser

from rgw_client import RGWClient
import json
import pandas as pd
from pathlib import Path
import csv
import os
import re
import unicodedata

#config path
CONFIG_FILE = 'pathofconfig/app.config'
#set the path instrument_raw_data
directory = "pathofdir/simulate_raw_data/instrument_raw_data"



bucket_history="bucket_history.json"



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
    
    # setting dictionary
    settings = {
        'USER_ID': user_id,
        'S3_ENDPOINT_URL': s3_endpoint_url,
        'ACCESS_KEY': access_key,
        'SECRET_KEY': secret_key,
        'REGION': region
    }
    
    return settings


settings = load_config(CONFIG_FILE)

# --- CONFIG---
RGW_ENDPOINT = settings['S3_ENDPOINT_URL']
ACCESS_KEY = settings['ACCESS_KEY']
SECRET_KEY = settings['SECRET_KEY']
USER_ID = settings['USER_ID']

# --- CLIENT ---
rgw = RGWClient(RGW_ENDPOINT, ACCESS_KEY, SECRET_KEY, verify=False)

# --- FUNCTIONS ---




def list_buckets():
    

    try:
        buckets = rgw.list_buckets()
    except:
        buckets=[]

    return buckets

buckets=list_buckets()
print(buckets)


proposal = [
    name for name in os.listdir(directory)
    if os.path.isdir(os.path.join(directory, name))
]
for item in proposal:
    if item not in buckets:
        print(item)
        bucket_name=item
        try:
            bucket = rgw.create_bucket(bucket_name)
            print(f'bucket {bucket_name} created')
        except:
           print('error')






