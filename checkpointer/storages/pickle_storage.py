import pickle
from pathlib import Path
from datetime import datetime
from ..types import Storage

def get_path(path: Path):
  return path.with_name(f"{path.name}.pkl")

class PickleStorage(Storage):
  def exists(self, path):
    return get_path(path).exists()

  def checkpoint_date(self, path):
    return datetime.fromtimestamp(get_path(path).stat().st_mtime)

  def store(self, path, data):
    full_path = get_path(path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with full_path.open("wb") as file:
      pickle.dump(data, file, -1)

  def load(self, path):
    full_path = get_path(path)
    with full_path.open("rb") as file:
      return pickle.load(file)

  def delete(self, path):
    try:
      get_path(path).unlink()
    except FileNotFoundError:
      pass
