# Watchdog can be used to listen for changes on metadata files
# https://pythonhosted.org/watchdog/

from collections import namedtuple
from pathlib import Path
from relib import hashing
from . import storage
from .function_body import get_function_hash
from functools import wraps

func_by_wrapper = {}
default_dir = str(Path.home()) + '/.checkpoints'

def get_invoke_path(func, function_hash, args, kwargs, path):
  if type(path) == str:
    return path
  elif callable(path):
    return path(*args, **kwargs)
  else:
    hash = hashing.hash([function_hash, args, kwargs or 0])
    file_name = func.__code__.co_filename.split('/')[-1]
    name = func.__name__
    return file_name + '/' + name + '/' + hash

def create_checkpointer_from_config(config):
  def checkpoint(opt_func=None, format='pickle', path=None, should_expire=None, when=True):
    def receive_func(func):
      if not (config.when and when):
        return func

      unwrapped_func = func_by_wrapper.get(func, func)
      function_hash = get_function_hash(unwrapped_func, func_by_wrapper)

      @wraps(func)
      def wrapper(*args, **kwargs):
        recheck = 'recheck' in kwargs and kwargs['recheck']

        if 'recheck' in kwargs:
          del kwargs['recheck']

        compute = lambda: func(*args, **kwargs)
        invoke_path = get_invoke_path(unwrapped_func, function_hash, args, kwargs, path)
        return storage.store_on_demand(compute, invoke_path, config, format, recheck, should_expire)

      wrapper.__name__ = unwrapped_func.__name__ + '_wrapper'
      func_by_wrapper[wrapper] = unwrapped_func
      return wrapper

    return receive_func(opt_func) if callable(opt_func) else receive_func

  return checkpoint

def create_checkpointer(dir=default_dir, when=True, verbosity=1):
  dir = dir + '/'
  opts = locals()
  CheckpointerConfig = namedtuple('CheckpointerConfig', sorted(opts))
  config = CheckpointerConfig(**opts)
  return create_checkpointer_from_config(config)

def read_only(wrapper_func, format='pickle', path=None):
  func = func_by_wrapper[wrapper_func]
  function_hash = get_function_hash(func, func_by_wrapper)

  def wrapper(*args, **kwargs):
    invoke_path = get_invoke_path(func, function_hash, args, kwargs, path)
    return storage.read_from_store(invoke_path, storage=format)

  return wrapper
