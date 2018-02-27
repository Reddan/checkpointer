import os
import pickle
from relib import imports
from datetime import datetime
from ..env import storage_dir

def get_collection_timestamp(path):
  full_path = storage_dir + path
  with open(full_path + '_meta.pkl', 'rb') as file:
    meta_data = pickle.load(file)
    return meta_data['created']

def get_is_expired(path):
  try:
    get_collection_timestamp(path)
    return False
  except:
    return True

def should_expire(path, expire_fn):
  return expire_fn(get_collection_timestamp(path))

def store_data(path, data):
  created = datetime.now()
  meta_data = {'created': created}
  full_path = storage_dir + path
  full_dir = '/'.join(full_path.split('/')[:-1])
  imports.ensure_dir(full_dir)
  with open(full_path + '.pkl', 'wb') as file:
    pickle.dump(data, file, -1)
  with open(full_path + '_meta.pkl', 'wb') as file:
    pickle.dump(meta_data, file, -1)
  return data

def load_data(path):
  full_path = storage_dir + path
  with open(full_path + '.pkl', 'rb') as file:
    return pickle.load(file)

def delete_data(path):
  full_path = storage_dir + path
  try:
    os.remove(full_path + '_meta.pkl')
    os.remove(full_path + '.pkl')
  except FileNotFoundError:
    pass
