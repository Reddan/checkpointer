from datetime import datetime
from ..types import Storage

store = {}
date_stored = {}

class MemoryStorage(Storage):
  @staticmethod
  def is_expired(path):
    return path not in store

  @staticmethod
  def should_expire(path, expire_fn):
    return expire_fn(date_stored[path])

  @staticmethod
  def store_data(path, data):
    store[path] = data
    date_stored[path] = datetime.now()
    return data

  @staticmethod
  def load_data(path):
    return store[path]

  @staticmethod
  def delete_data(path):
    del store[path]
    del date_stored[path]
