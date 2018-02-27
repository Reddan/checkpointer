from .storages import memory_storage, pickle_storage, bcolz_storage, mongo_storage
from termcolor import colored

storages = {
  'memory': memory_storage,
  'pickle': pickle_storage,
  'bcolz': bcolz_storage,
  'mongo': mongo_storage
}

initialized_storages = set()

def init_storage(storage):
  if storage not in initialized_storages:
    storage.initialize()
    initialized_storages.add(storage)

def log(color, title, invoke_level, name):
  title_log = colored(title, 'grey', 'on_' + color)
  invoke_level_log = (' ' * min(1, invoke_level)) + ('──' * invoke_level)
  rest_log = colored(invoke_level_log + ' ' + name, color)
  print(title_log + rest_log)

def store_on_demand(func, name, storage='pickle', force=False, should_expire=None, invoke_level=0):
  print(force)
  if type(storage) == str:
    storage = storages[storage]
  init_storage(storage)
  do_print = storage != memory_storage
  refresh = force or storage.get_is_expired(name) or (should_expire and storage.should_expire(name, should_expire))

  if refresh:
    if do_print: log('blue', ' MEMORIZING ', invoke_level, name)
    data = func()
    return storage.store_data(name, data)
  else:
    try:
      data = storage.load_data(name)
      if do_print: log('green', ' REMEMBERED ', invoke_level, name)
      return data
    except (EOFError, FileNotFoundError):
      storage.delete_data(name)
      print(name + ' corrupt, removing')
      return store_on_demand(func, name, storage, should_expire, invoke_level)

def read_from_store(name, storage='pickle'):
  if type(storage) == str:
    storage = storages[storage]
  init_storage(storage)
  try:
    return storage.load_data(name)
  except:
    return None
