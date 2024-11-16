import pickle
from pathlib import Path
from datetime import datetime
from ..types import Storage

def get_paths(path: Path):
  meta_full_path = path.with_name(f"{path.name}_meta.pkl")
  pkl_full_path = path.with_name(f"{path.name}.pkl")
  return meta_full_path, pkl_full_path

def get_collection_timestamp(path: Path):
  meta_full_path, _ = get_paths(path)
  with meta_full_path.open("rb") as file:
    meta_data = pickle.load(file)
    return meta_data["created"]

class PickleStorage(Storage):
  @staticmethod
  def is_expired(path):
    try:
      get_collection_timestamp(path)
      return False
    except (FileNotFoundError, EOFError):
      return True

  @staticmethod
  def should_expire(path, expire_fn):
    return expire_fn(get_collection_timestamp(path))

  @staticmethod
  def store_data(path, data):
    created = datetime.now()
    meta_data = {"created": created} # TODO: this should just be a JSON or binary dump of the unix timestamp and other metadata - not pickle
    meta_full_path, pkl_full_path = get_paths(path)
    pkl_full_path.parent.mkdir(parents=True, exist_ok=True)
    with pkl_full_path.open("wb") as file:
      pickle.dump(data, file, -1)
    with meta_full_path.open("wb") as file:
      pickle.dump(meta_data, file, -1)
    return data

  @staticmethod
  def load_data(path):
    _, full_path = get_paths(path)
    with full_path.open("rb") as file:
      return pickle.load(file)

  @staticmethod
  def delete_data(path):
    meta_full_path, pkl_full_path = get_paths(path)
    try:
      meta_full_path.unlink()
      pkl_full_path.unlink()
    except FileNotFoundError:
      pass
