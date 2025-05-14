import pickle
import shutil
from datetime import datetime
from pathlib import Path
from .storage import Storage

def filedate(path: Path) -> datetime:
  return datetime.fromtimestamp(path.stat().st_mtime)

class PickleStorage(Storage):
  def get_path(self, call_id: str):
    return self.fn_dir() / f"{call_id}.pkl"

  def store(self, call_id, data):
    path = self.get_path(call_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
      pickle.dump(data, file, -1)
    return data

  def exists(self, call_id):
    return self.get_path(call_id).exists()

  def checkpoint_date(self, call_id):
    # Should use st_atime/access time?
    return filedate(self.get_path(call_id))

  def load(self, call_id):
    with self.get_path(call_id).open("rb") as file:
      return pickle.load(file)

  def delete(self, call_id):
    self.get_path(call_id).unlink(missing_ok=True)

  def cleanup(self, invalidated=True, expired=True):
    version_path = self.fn_dir()
    fn_path = version_path.parent
    if invalidated:
      old_dirs = [path for path in fn_path.iterdir() if path.is_dir() and path != version_path]
      for path in old_dirs:
        shutil.rmtree(path)
      print(f"Removed {len(old_dirs)} invalidated directories for {self.cached_fn.__qualname__}")
    if expired and self.checkpointer.should_expire:
      count = 0
      for pkl_path in fn_path.glob("**/*.pkl"):
        if self.checkpointer.should_expire(filedate(pkl_path)):
          count += 1
          pkl_path.unlink(missing_ok=True)
      print(f"Removed {count} expired checkpoints for {self.cached_fn.__qualname__}")
