from datetime import datetime
from ..types import Storage

store = {}
date_stored = {}

class MemoryStorage(Storage):
  @staticmethod
  def exists(path):
    return str(path) in store

  @staticmethod
  def checkpoint_date(path):
    return date_stored[str(path)]

  @staticmethod
  def store(path, data):
    store[str(path)] = data
    date_stored[str(path)] = datetime.now()

  @staticmethod
  def load(path):
    return store[str(path)]

  @staticmethod
  def delete(path):
    del store[str(path)]
    del date_stored[str(path)]
