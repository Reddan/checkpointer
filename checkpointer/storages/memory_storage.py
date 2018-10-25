from datetime import datetime

store = {}
date_stored = {}

def get_is_expired(config, path):
  return path not in store

def should_expire(config, path, expire_fn):
  return expire_fn(date_stored[path])

def store_data(config, path, data):
  store[path] = data
  date_stored[path] = datetime.now()
  return data

def load_data(config, path):
  return store[path]
