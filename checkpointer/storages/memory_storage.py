from datetime import datetime

store = {}
date_stored = {}

def get_is_expired(path):
  return path not in store

def should_expire(path, expire_fn):
  return expire_fn(date_stored[path])

def store_data(path, data):
  store[path] = data
  date_stored[path] = datetime.now()
  return data

def load_data(path):
  return store[path]
