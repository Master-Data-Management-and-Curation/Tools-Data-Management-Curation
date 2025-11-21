import numpy as np
import h5py
from nexusformat.nexus import *
import tifffile
from ncempy.io import dm
import datetime
from datetime import datetime


def create_NXem(bucket_name,key,dm3_file, user,sample_name, description,index,additional,path_nx):
  
  file_data = dm.dmReader(dm3_file)
  img = file_data['data']
  pixelSize=file_data['pixelSize']
  pixelUnit=file_data['pixelUnit']

  filename = bucket_name+'-'+key+".nxs"
  f = h5py.File(path_nx+'/'+filename, "w")
  f.attrs['default'] = 'entry'
  f.create_group("entry")
  f['/entry'].attrs["NX_class"] = "NXentry"
  f['/entry'].create_dataset('definition',data='NXem')

# /entry/duration
  f['/entry'].create_dataset('experiment_description',data=description)
  f['/entry/experiment_description'].attrs["description"] = 'SEM'


  f['/entry'].create_dataset('title',data=filename)
  f['/entry'].create_group("sample")
  f['/entry/sample'].attrs["NX_class"] = "NXsample"

# /entry/sample/name
  f['/entry/sample'].create_dataset('name',data=sample_name)
  f['/entry/sample'].create_dataset('index',data=index)
  f['/entry/sample/index'].attrs['units']='K'
  f['/entry/sample'].create_dataset('additional',data=additional)

  f['/entry'].create_group("user")
  f['/entry/user'].attrs["NX_class"] = "NXuser"

# \entry\user\name
  f['/entry/user'].create_dataset('name',data=user)
  f['/entry'].create_group("data")
  f['/entry/data'].attrs["NX_class"] = "NXdata"
#f['/entry/data'].attrs["axes"] = "energy"
  f['/entry/data'].create_dataset('img',data=img)
  f['/entry/data'].attrs["signal"] = "img"
  f['/entry/data'].attrs["axes"] =['X','Y']

  f.close()
  return f             


