import inspect
import asyncio
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

      config_ = config._replace(format=format)
      is_async = inspect.iscoroutinefunction(func)
      unwrapped_func = unwrap_func(func)
      function_hash = get_function_hash(unwrapped_func)

      @wraps(unwrapped_func)
      async def wrapper(*args, **kwargs):
        async def get_data():
          if is_async:
            return await func(*args, **kwargs)
          else:
            return func(*args, **kwargs)

        recheck = kwargs.pop('recheck', False)
        invoke_path = get_invoke_path(unwrapped_func, function_hash, args, kwargs, path)
        return await storage.store_on_demand(get_data, invoke_path, config_, recheck, should_expire)

      wrapper.checkpoint_config = config_

      if is_async == False:
        def sync_wrapper(*args, **kwargs):
          return asyncio.get_event_loop().run_until_complete(wrapper(*args, **kwargs))

        sync_wrapper.checkpoint_config = config_

        return sync_wrapper

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
