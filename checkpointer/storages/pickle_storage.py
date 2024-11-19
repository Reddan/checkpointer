import pickle
from pathlib import Path
from datetime import datetime
from ..types import Storage

def get_path(path: Path):
  return path.with_name(f"{path.name}.pkl")

class PickleStorage(Storage):
  @staticmethod
  def exists(path):
    return get_path(path).exists()

  @staticmethod
  def checkpoint_date(path):
    return datetime.fromtimestamp(get_path(path).stat().st_mtime)

  @staticmethod
  def store(path, data):
    full_path = get_path(path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with full_path.open("wb") as file:
      pickle.dump(data, file, -1)

  @staticmethod
  def load(path):
    full_path = get_path(path)
    with full_path.open("rb") as file:
      return pickle.load(file)

  @staticmethod
  def delete(path):
    try:
      get_path(path).unlink()
    except FileNotFoundError:
      pass
