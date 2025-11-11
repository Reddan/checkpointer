import pickle
import shutil
from datetime import datetime
from io import BytesIO
from pathlib import Path
from ..utils import clear_directory
from .storage import Storage

try:
  import polars as pl
except:
  pl = None

def filedate(path: Path) -> datetime:
  return datetime.fromtimestamp(path.stat().st_mtime)

class ExtendedPickler(pickle.Pickler):
  def reducer_override(self, obj): # type: ignore
    if pl and isinstance(obj, pl.DataFrame):
      buffer = BytesIO()
      obj.rechunk().write_parquet(buffer)
      return pl.read_parquet, (buffer.getvalue(),)
    return NotImplemented

class PickleStorage(Storage):
  def get_path(self, call_hash: str):
    return self.fn_dir() / f"{call_hash[:2]}/{call_hash[2:]}.pkl"

  def store(self, call_hash, data):
    path = self.get_path(call_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
      ExtendedPickler(file, -1).dump(data)
    return data

  def exists(self, call_hash):
    return self.get_path(call_hash).exists()

  def checkpoint_date(self, call_hash):
    # Should use st_atime/access time?
    return filedate(self.get_path(call_hash))

  def load(self, call_hash):
    with self.get_path(call_hash).open("rb") as file:
      return pickle.load(file)

  def delete(self, call_hash):
    self.get_path(call_hash).unlink(missing_ok=True)

  def cleanup(self, invalidated=True, expired=True):
    version_path = self.fn_dir()
    fn_path = version_path.parent
    if invalidated and fn_path.exists():
      invalidated_dirs = [path for path in fn_path.iterdir() if path.is_dir() and path != version_path]
      pkls = [pkl for path in invalidated_dirs for pkl in path.glob("**/*.pkl")]
      for pkl in pkls:
        pkl.unlink(missing_ok=True)
      if pkls:
        print(f"Removed {len(pkls)} checkpoints from {len(invalidated_dirs)} invalidated directories for {self.cached_fn.__qualname__}")
    if expired and self.checkpointer.expiry:
      count = 0
      for pkl in fn_path.glob("**/*.pkl"):
        if self.expired_dt(filedate(pkl)):
          count += 1
          pkl.unlink(missing_ok=True)
      if count:
        print(f"Removed {count} expired checkpoints for {self.cached_fn.__qualname__}")
    clear_directory(fn_path)

  def clear(self):
    fn_path = self.fn_dir().parent
    if fn_path.exists():
      shutil.rmtree(fn_path)
