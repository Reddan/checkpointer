import shutil
from pathlib import Path
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

def get_paths(root_path, invoke_path):
  full_path = Path(invoke_path) if root_path is None else root_path / invoke_path
  meta_full_path = full_path.with_name(full_path.name + '_meta')
  return full_path, meta_full_path

def get_collection_timestamp(config, path):
  import bcolz
  _, meta_full_path = get_paths(config.root_path, path)
  meta_data = bcolz.open(meta_full_path)[:][0]
  return meta_data['created']

def get_is_expired(config, path):
  try:
    get_collection_timestamp(config, path)
    return False
  except FileNotFoundError:
    return True

def should_expire(config, path, expire_fn):
  return expire_fn(get_collection_timestamp(config, path))

def insert_data(path, data):
  import bcolz
  c = bcolz.carray(data, rootdir=path, mode='w')
  c.flush()

def store_data(config, path, data, expire_in=None):
  full_path, meta_full_path = get_paths(config.root_path, path)
  full_path.parent.mkdir(parents=True, exist_ok=True)
  created = datetime.now()
  data_type_str = get_data_type_str(data)
  if data_type_str == 'tuple':
    fields = list(range(len(data)))
  elif data_type_str == 'dict':
    fields = sorted(data.keys())
  else:
    fields = []
  meta_data = {'created': created, 'data_type_str': data_type_str, 'fields': fields}
  insert_data(meta_full_path, meta_data)
  if data_type_str in ['tuple', 'dict']:
    for i in range(len(fields)):
      sub_path = f"{path} ({i})"
      store_data(config, sub_path, data[fields[i]])
  else:
    insert_data(full_path, data)
  return data

def load_data(config, path):
  import bcolz
  full_path, meta_full_path = get_paths(config.root_path, path)
  meta_data = bcolz.open(meta_full_path)[:][0]
  data_type_str = meta_data['data_type_str']
  if data_type_str in ['tuple', 'dict']:
    fields = meta_data['fields']
    partitions = range(len(fields))
    data = [load_data(config, f"{path} ({i})") for i in partitions]
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
  full_path, meta_full_path = get_paths(config.root_path, path)
  try:
    shutil.rmtree(meta_full_path)
    shutil.rmtree(full_path)
  except FileNotFoundError:
    pass
