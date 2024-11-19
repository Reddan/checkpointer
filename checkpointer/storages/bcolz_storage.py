import shutil
from pathlib import Path
from datetime import datetime
from ..types import Storage

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
  @staticmethod
  def exists(path):
    return path.exists()

  @staticmethod
  def checkpoint_date(path):
    return datetime.fromtimestamp(path.stat().st_mtime)

  @staticmethod
  def store(path, data):
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
        BcolzStorage.store(child_path, data[fields[i]])
    else:
      insert_data(path, data)

  @staticmethod
  def load(path):
    import bcolz
    metapath = get_metapath(path)
    meta_data = bcolz.open(metapath)[:][0]
    data_type_str = meta_data["data_type_str"]
    if data_type_str in ["tuple", "dict"]:
      fields = meta_data["fields"]
      partitions = range(len(fields))
      data = [BcolzStorage.load(Path(f"{path} ({i})")) for i in partitions]
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

  @staticmethod
  def delete(path):
    # NOTE: Not recursive
    metapath = get_metapath(path)
    try:
      shutil.rmtree(metapath)
      shutil.rmtree(path)
    except FileNotFoundError:
      pass
