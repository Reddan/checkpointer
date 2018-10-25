from termcolor import colored
from .storages import memory_storage, pickle_storage, bcolz_storage

storages = {
  'memory': memory_storage,
  'pickle': pickle_storage,
  'bcolz': bcolz_storage,
}

initialized_storages = set()
invoke_level = {'val': -1}

def log(color, title, invoke_level, name):
  title_log = colored(title, 'grey', 'on_' + color)
  invoke_level_log = (' ' * min(1, invoke_level)) + ('──' * invoke_level)
  rest_log = colored(invoke_level_log + ' ' + name, color)
  print(title_log + rest_log)

def get_storage(storage):
  if type(storage) == str:
    storage = storages[storage]
  if storage not in initialized_storages:
    if hasattr(storage, 'initialize'):
      storage.initialize()
    initialized_storages.add(storage)
  return storage

def store_on_demand(func, name, config, storage='pickle', force=False, should_expire=None):
  try:
    invoke_level['val'] += 1
    storage = get_storage(storage)
    do_print = storage != memory_storage and config.verbosity != 0
    refresh = force \
      or storage.get_is_expired(config, name) \
      or (should_expire and storage.should_expire(config, name, should_expire))

    if refresh:
      if do_print: log('blue', ' MEMORIZING ', invoke_level['val'], name)
      data = func()
      return storage.store_data(config, name, data)
    else:
      try:
        data = storage.load_data(config, name)
        if do_print: log('green', ' REMEMBERED ', invoke_level['val'], name)
        return data
      except (EOFError, FileNotFoundError):
        storage.delete_data(config, name)
        print(name + ' corrupt, removing')
        return store_on_demand(func, name, storage, force, should_expire, invoke_level['val'])
  finally:
    invoke_level['val'] -= 1

def read_from_store(name, storage='pickle'):
  storage = get_storage(storage)
  try:
    return storage.load_data(name)
  except:
    return None
