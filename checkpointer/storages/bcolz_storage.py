import shutil
from pathlib import Path
from datetime import datetime
from .storage import Storage

def get_data_type_str(x):
  if isinstance(x, tuple):
    return "tuple"
  elif isinstance(x, dict):
    return "dict"
  elif isinstance(x, list):
    return "list"
  elif isinstance(x, str) or not hasattr(x, "__len__"):
    return "other"
  else:
    return "ndarray"

def get_metapath(path: Path):
  return path.with_name(f"{path.name}_meta")

def insert_data(path: Path, data):
  import bcolz
  c = bcolz.carray(data, rootdir=path, mode="w")
  c.flush()

class BcolzStorage(Storage):
  def exists(self, path):
    return path.exists()

  def checkpoint_date(self, path):
    return datetime.fromtimestamp(path.stat().st_mtime)

  def store(self, path, data):
    metapath = get_metapath(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data_type_str = get_data_type_str(data)
    if data_type_str == "tuple":
      fields = list(range(len(data)))
    elif data_type_str == "dict":
      fields = sorted(data.keys())
    else:
      fields = []
    meta_data = {"data_type_str": data_type_str, "fields": fields}
    insert_data(metapath, meta_data)
    if data_type_str in ["tuple", "dict"]:
      for i in range(len(fields)):
        child_path = Path(f"{path} ({i})")
        self.store(child_path, data[fields[i]])
    else:
      insert_data(path, data)

  def load(self, path):
    import bcolz
    metapath = get_metapath(path)
    meta_data = bcolz.open(metapath)[:][0]
    data_type_str = meta_data["data_type_str"]
    if data_type_str in ["tuple", "dict"]:
      fields = meta_data["fields"]
      partitions = range(len(fields))
      data = [self.load(Path(f"{path} ({i})")) for i in partitions]
      if data_type_str == "tuple":
        return tuple(data)
      else:
        return dict(zip(fields, data))
    else:
      data = bcolz.open(path)
      if data_type_str == "list":
        return list(data)
      elif data_type_str == "other":
        return data[0]
      else:
        return data[:]

  def delete(self, path):
    # NOTE: Not recursive
    shutil.rmtree(get_metapath(path), ignore_errors=True)
    shutil.rmtree(path, ignore_errors=True)

  def cleanup(self, invalidated=True, expired=True):
    raise NotImplementedError("cleanup() not implemented for bcolz storage")
