import pickle
import shutil
from pathlib import Path
from datetime import datetime
from .storage import Storage

def get_path(path: Path):
  return path.with_name(f"{path.name}.pkl")

class PickleStorage(Storage):
  def store(self, path, data):
    full_path = get_path(path)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    with full_path.open("wb") as file:
      pickle.dump(data, file, -1)

  def exists(self, path):
    return get_path(path).exists()

  def checkpoint_date(self, path):
    # Should use st_atime/access time?
    return datetime.fromtimestamp(get_path(path).stat().st_mtime)

  def load(self, path):
    with get_path(path).open("rb") as file:
      return pickle.load(file)

  def delete(self, path):
    get_path(path).unlink(missing_ok=True)

  def cleanup(self, invalidated=True, expired=True):
    version_path = self.checkpointer.root_path.resolve() / self.checkpoint_fn.fn_dir / self.checkpoint_fn.fn_hash
    fn_path = version_path.parent
    if invalidated:
      old_dirs = [path for path in fn_path.iterdir() if path.is_dir() and path != version_path]
      for path in old_dirs:
        shutil.rmtree(path)
      print(f"Removed {len(old_dirs)} invalidated directories for {self.checkpoint_fn.__qualname__}")
    if expired and self.checkpointer.should_expire:
      count = 0
      for pkl_path in fn_path.rglob("*.pkl"):
        path = pkl_path.with_suffix("")
        if self.checkpointer.should_expire(self.checkpoint_date(path)):
          count += 1
          self.delete(path)
      print(f"Removed {count} expired checkpoints for {self.checkpoint_fn.__qualname__}")
