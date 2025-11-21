

import configparser
from rgw_client import RGWClient
import json
import pandas as pd
from pathlib import Path
import csv
import os
import re
import unicodedata


# config_path
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
    
    # Dictionary
    settings = {
        'USER_ID': user_id,
        'S3_ENDPOINT_URL': s3_endpoint_url,
        'ACCESS_KEY': access_key,
        'SECRET_KEY': secret_key,
        'REGION': region
    }
    
    return settings



settings = load_config(CONFIG_FILE)

# --- CONFIG ---
RGW_ENDPOINT = settings['S3_ENDPOINT_URL']
ACCESS_KEY = settings['ACCESS_KEY']
SECRET_KEY = settings['SECRET_KEY']
USER_ID = settings['USER_ID']

# --- CLIENT ---
rgw = RGWClient(RGW_ENDPOINT, ACCESS_KEY, SECRET_KEY, verify=False)

# --- FUNCTION--



import unicodedata

path_buckets_file=path_base+'/'+'storage'
buckets_file='buckets_fill.json'

with open(path_buckets_file+'/'+buckets_file, "r") as f:
    data = json.load(f)
    
path_tmp=path_base+'/nexus_preparation/tmp'
list_nx=os.listdir(path_tmp)
for bucket_name in data['buckets']:
    i=0
    obj=rgw.list_objects(bucket_name)
    for file in list_nx:
            if file.split('-')[0]+'-'+file.split('-')[1]==bucket_name:
                if file not in obj and i==0:
                    rgw.upload_file(bucket_name, path_tmp+'/'+file)
                    i=1

    

