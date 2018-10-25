import shutil
from relib import imports
from datetime import datetime

def get_data_type_str(x):
  if isinstance(x, tuple):
    return 'tuple'
  elif isinstance(x, dict):
    return 'dict'
  elif isinstance(x, list):
    return 'list'
  elif isinstance(x, str) or not hasattr(x, '__len__'):
    return 'other'
  else:
    return 'ndarray'

def get_collection_timestamp(config, path):
  import bcolz
  full_path = config.dir + path
  meta_data = bcolz.open(full_path + '_meta')[:][0]
  return meta_data['created']

def get_is_expired(config, path):
  try:
    get_collection_timestamp(config, path)
    return False
  except:
    return True

def should_expire(config, path, expire_fn):
  return expire_fn(get_collection_timestamp(config, path))

def insert_data(path, data):
  import bcolz
  c = bcolz.carray(data, rootdir=path, mode='w')
  c.flush()

def store_data(config, path, data, expire_in=None):
  full_path = config.dir + path
  full_dir = '/'.join(full_path.split('/')[:-1])
  imports.ensure_dir(full_dir)
  created = datetime.now()
  data_type_str = get_data_type_str(data)
  if data_type_str == 'tuple':
    fields = list(range(len(data)))
  elif data_type_str == 'dict':
    fields = sorted(data.keys())
  else:
    fields = []
  meta_data = {'created': created, 'data_type_str': data_type_str, 'fields': fields}
  insert_data(full_path + '_meta', meta_data)
  if data_type_str in ['tuple', 'dict']:
    for i in range(len(fields)):
      sub_path = path + ' (' + str(i) + ')'
      store_data(config, sub_path, data[fields[i]])
  else:
    insert_data(full_path, data)
  return data

def load_data(config, path):
  import bcolz
  full_path = config.dir + path
  meta_data = bcolz.open(full_path + '_meta')[:][0]
  data_type_str = meta_data['data_type_str']
  if data_type_str in ['tuple', 'dict']:
    fields = meta_data['fields']
    partitions = range(len(fields))
    data = [load_data(config, path + ' (' + str(i) + ')') for i in partitions]
    if data_type_str == 'tuple':
      return tuple(data)
    else:
      return dict(zip(fields, data))
  else:
    data = bcolz.open(full_path)
    if data_type_str == 'list':
      return list(data)
    elif data_type_str == 'other':
      return data[0]
    else:
      return data[:]

def delete_data(config, path):
  full_path = config.dir + path
  try:
    shutil.rmtree(full_path + '_meta')
    shutil.rmtree(full_path)
  except FileNotFoundError:
    pass
