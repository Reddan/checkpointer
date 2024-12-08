from typing import Any
from pathlib import Path
from datetime import datetime
from .storage import Storage

item_map: dict[Path, dict[str, tuple[datetime, Any]]] = {}

def get_short_path(path: Path):
  return path.parts[-1]

class MemoryStorage(Storage):
  def get_dict(self):
    return item_map.setdefault(self.checkpointer.root_path / self.checkpoint_fn.fn_subdir, {})

  def store(self, path, data):
    self.get_dict()[get_short_path(path)] = (datetime.now(), data)

  def exists(self, path):
    return get_short_path(path) in self.get_dict()

  def checkpoint_date(self, path):
    return self.get_dict()[get_short_path(path)][0]

  def load(self, path):
    return self.get_dict()[get_short_path(path)][1]

  def delete(self, path):
    del self.get_dict()[get_short_path(path)]

  def cleanup(self, invalidated=True, expired=True):
    curr_key = self.checkpointer.root_path / self.checkpoint_fn.fn_subdir
    for key, calldict in list(item_map.items()):
      if key.parent == curr_key.parent:
        if invalidated and key != curr_key:
          del item_map[key]
        elif expired and self.checkpointer.should_expire:
          for callid, (date, _) in list(calldict.items()):
            if self.checkpointer.should_expire(date):
              del calldict[callid]
