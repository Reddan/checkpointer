# Watchdog can be used to listen for changes on metadata files
# https://pythonhosted.org/watchdog/

from collections import namedtuple
from pathlib import Path
import relib.hashing as hashing
from . import storage
from .function_body import get_function_hash
from functools import wraps
from .utils import unwrap_func

default_dir = str(Path.home()) + '/.checkpoints'

def get_invoke_path(func, function_hash, args, kwargs, path):
  if type(path) == str:
    return path
  elif callable(path):
    return path(*args, **kwargs)
  else:
    hash = hashing.hash([function_hash, args, kwargs or 0])
    file_name = Path(func.__code__.co_filename).name
    name = func.__name__
    return file_name + '/' + name + '/' + hash

def create_checkpointer_from_config(config):
  def checkpoint(opt_func=None, format=config.format, path=None, should_expire=None, when=True):
    def receive_func(func):
      if not (config.when and when):
        return func

      unwrapped_func = unwrap_func(func)
      function_hash = get_function_hash(unwrapped_func)

      @wraps(unwrapped_func)
      def wrapper(*args, **kwargs):
        if 'recheck' in kwargs:
          recheck = kwargs['recheck']
          del kwargs['recheck']
        else:
          recheck = False

        compute = lambda: func(*args, **kwargs)
        invoke_path = get_invoke_path(unwrapped_func, function_hash, args, kwargs, path)
        return storage.store_on_demand(compute, invoke_path, config, format, recheck, should_expire)

      # wrapper.__name__ = unwrapped_func.__name__ + '_wrapper'
      return wrapper

    return receive_func(opt_func) if callable(opt_func) else receive_func

  return checkpoint

def create_checkpointer(format='pickle', dir=default_dir, when=True, verbosity=1):
  dir = dir + '/'
  opts = locals()
  CheckpointerConfig = namedtuple('CheckpointerConfig', sorted(opts))
  config = CheckpointerConfig(**opts)
  return create_checkpointer_from_config(config)

def read_only(wrapper_func, config, format='pickle', path=None):
  func = unwrap_func(wrapper_func)
  function_hash = get_function_hash(func)

  def wrapper(*args, **kwargs):
    invoke_path = get_invoke_path(func, function_hash, args, kwargs, path)
    return storage.read_from_store(invoke_path, config, storage=format)

  return wrapper
