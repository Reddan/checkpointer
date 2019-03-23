import os
import errno

def ensure_dir(directory):
  if not os.path.exists(directory):
    try:
      os.makedirs(directory)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

def unwrap_func(func):
  if hasattr(func, '__wrapped__'):
    return unwrap_func(func.__wrapped__)
  else:
    return func
