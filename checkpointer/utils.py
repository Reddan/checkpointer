from pathlib import Path

def ensure_dir(directory):
  Path(directory).mkdir(parents=True, exist_ok=True)

def unwrap_func(func):
  if hasattr(func, '__wrapped__'):
    return unwrap_func(func.__wrapped__)
  else:
    return func
