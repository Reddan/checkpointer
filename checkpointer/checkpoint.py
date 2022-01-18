from relib import hashing
from . import storage
from .function_body import get_function_hash

func_by_wrapper = {}
invoke_level = -1

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

def checkpoint(opt_func=None, format='pickle', path=None, should_expire=None, when=True):
  def receive_func(func):
    if not when:
      return func
    unwrapped_func = func_by_wrapper.get(func, func)
    function_hash = get_function_hash(unwrapped_func, func_by_wrapper)

    def wrapper(*args, **kwargs):
      recheck = 'recheck' in kwargs and kwargs['recheck']
      if 'recheck' in kwargs:
        del kwargs['recheck']
      compute = lambda: func(*args, **kwargs)
      global invoke_level
      invoke_path = get_invoke_path(unwrapped_func, function_hash, args, kwargs, path)
      try:
        invoke_level += 1
        return storage.store_on_demand(compute, invoke_path, format, recheck, should_expire, invoke_level)
      finally:
        invoke_level -= 1

    wrapper.__name__ = unwrapped_func.__name__ + '_wrapper'
    func_by_wrapper[wrapper] = unwrapped_func
    return wrapper

  return receive_func(opt_func) if callable(opt_func) else receive_func

def read_only(wrapper_func, format='pickle', path=None):
  func = func_by_wrapper[wrapper_func]
  function_hash = get_function_hash(func, func_by_wrapper)

  def wrapper(*args, **kwargs):
    invoke_path = get_invoke_path(func, function_hash, args, kwargs, path)
    return storage.read_from_store(invoke_path, storage_format=format)

  return wrapper
