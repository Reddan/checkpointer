import os

def ensure_dir(directory):
  if not os.path.exists(directory):
    try:
      os.makedirs(directory)
    except OSError as e:
      if e.errno != os.errno.EEXIST:
        raise
