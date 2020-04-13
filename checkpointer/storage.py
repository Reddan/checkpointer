import inspect
from termcolor import colored
from .storages import memory_storage, pickle_storage, bcolz_storage

storages = {
  'memory': memory_storage,
  'pickle': pickle_storage,
  'bcolz': bcolz_storage,
}

initialized_storages = set()

def create_logger(should_log):
  def log(color, title, text):
    if should_log:
      title_log = colored(f' {title} ', 'grey', 'on_' + color)
      rest_log = colored(text, color)
      print(title_log + ' ' + rest_log)
  return log

def get_storage(storage):
  if type(storage) == str:
    storage = storages[storage]
  if storage not in initialized_storages:
    if hasattr(storage, 'initialize'):
      storage.initialize()
    initialized_storages.add(storage)
  return storage

async def store_on_demand(get_data, name, config, force=False, should_expire=None):
  storage = get_storage(config.format)
  should_log = storage != memory_storage and config.verbosity != 0
  log = create_logger(should_log)
  refresh = force \
    or storage.get_is_expired(config, name) \
    or (should_expire and storage.should_expire(config, name, should_expire))

  if refresh:
    log('blue', 'MEMORIZING', name)
    data = get_data()
    if inspect.iscoroutine(data):
      data = await data
    return storage.store_data(config, name, data)
  else:
    try:
      data = storage.load_data(config, name)
      log('green', 'REMEMBERED', name)
      return data
    except (EOFError, FileNotFoundError):
      log('yellow', 'CORRUPTED', name)
      storage.delete_data(config, name)
      result = await store_on_demand(get_data, name, config, force, should_expire)
      return result

def read_from_store(name, config, storage='pickle'):
  storage = get_storage(storage)
  try:
    return storage.load_data(config, name)
  except:
    return None
