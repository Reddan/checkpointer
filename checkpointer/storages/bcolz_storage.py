import bcolz
import shutil
from relib import imports
from datetime import datetime
from ..env import storage_dir

def get_collection_timestamp(path):
  full_path = storage_dir + path
  meta_data = bcolz.open(full_path + '_meta')[:][0]
  return meta_data['created']

def get_is_expired(path):
  try:
    get_collection_timestamp(path)
    return False
  except:
    return True

def should_expire(path, expire_fn):
  return expire_fn(get_collection_timestamp(path))

def insert_data(path, data):
  c = bcolz.carray(data, rootdir=path, mode='w')
  c.flush()

def store_data(path, data, expire_in=None):
  full_path = storage_dir + path
  full_dir = '/'.join(full_path.split('/')[:-1])
  imports.ensure_dir(full_dir)
  created = datetime.now()
  is_tuple = isinstance(data, tuple)
  is_dict = isinstance(data, dict)
  is_not_list = is_tuple or is_dict or isinstance(data, str) or not hasattr(data, '__len__')
  if is_tuple:
    fields = list(range(len(data)))
  elif is_dict:
    fields = sorted(data.keys())
  else:
    fields = []
  meta_data = {'created': created, 'is_tuple': is_tuple, 'is_dict': is_dict, 'is_not_list': is_not_list, 'fields': fields}
  insert_data(full_path + '_meta', meta_data)
  if is_tuple or is_dict:
    for i in range(len(fields)):
      sub_path = path + ' (' + str(i) + ')'
      store_data(sub_path, data[fields[i]])
  else:
    insert_data(full_path, data)
  return data

def load_data(path):
  full_path = storage_dir + path
  meta_data = bcolz.open(full_path + '_meta')[:][0]
  if meta_data['is_tuple'] or meta_data['is_dict']:
    fields = meta_data['fields']
    partitions = range(len(fields))
    data = [load_data(path + ' (' + str(i) + ')') for i in partitions]
    if meta_data['is_tuple']:
      return tuple(data)
    else:
      return dict(zip(fields, data))
  elif meta_data['is_not_list']:
    return bcolz.open(full_path)[0]
  else:
    return bcolz.open(full_path)[:]

def delete_data(path):
  full_path = storage_dir + path
  try:
    shutil.rmtree(full_path + '_meta')
    shutil.rmtree(full_path)
  except FileNotFoundError:
    pass
