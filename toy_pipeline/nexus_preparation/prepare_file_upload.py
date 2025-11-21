import os
import json



import numpy as np
import h5py
from nexusformat.nexus import *
import tifffile
from ncempy.io import dm
import datetime
from datetime import datetime
from create_nexus import create_NXem
import uuid

  

# --- CONFIG ---
path_base='/Users/federica/Documents/Esercizi_MDMC25/pipeline_pseudo_ofed'
buckets_file = "buckets_fill.json"
path_buckets_file=path_base+"/storage"
path_nx=path_base+"/nexus_preparation/tmp"
data_directory = path_base+"/simulate_raw_data/instrument_raw_data"
metadata_directory = path_base+"/simulate_raw_data/eln_data"  
# --------------


# JSON upload
with open(path_buckets_file+'/'+buckets_file, "r") as f:
    data = json.load(f)

for bucket_name in data['buckets']:
     i=0
     bucket_path = os.path.join(data_directory, bucket_name)
     if os.path.isdir(bucket_path):
         print(os.listdir(bucket_path))
         i+=1
     eln_path = os.path.join(metadata_directory, bucket_name+'-eln.json')
     if os.path.isfile(eln_path):
         print('eln trovato')
         i+=1
         print(i)
     if i==2:
         print("creo_nexus")
         with open(eln_path, "r") as f:
           metadata = json.load(f)
           user=metadata[bucket_name]
           samples=metadata['sample']
           for item in samples:
               key = str(uuid.uuid4())
               sample_name=list(item.keys())[0]
               file_name=item[sample_name]['file_name']
               description=item[sample_name]['description']
               index=item[sample_name]['index']
               additional=item[sample_name]['additional']
               
               dm3_file=data_directory+'/'+ bucket_name+'/'+file_name
               try:
                 create_NXem(bucket_name,key,dm3_file, user,sample_name, description,index,additional, path_nx)
               except:
                   print('impossible for sample:',sample_name)




        #creo nexus



