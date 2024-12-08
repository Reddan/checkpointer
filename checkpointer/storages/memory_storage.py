from typing import Any
from pathlib import Path
from datetime import datetime
from ..types import Storage

item_map: dict[Path, dict[str, tuple[datetime, Any]]] = {}

class MemoryStorage(Storage):
  def get_dict(self):
    return item_map.setdefault(self.checkpointer.root_path / self.checkpoint_fn.fn_subdir, {})

  def get_short_path(self, path: Path):
    return path.parts[-1]

  def exists(self, path):
    return self.get_short_path(path) in self.get_dict()

  def checkpoint_date(self, path):
    return self.get_dict()[self.get_short_path(path)][0]

  def store(self, path, data):
    self.get_dict()[self.get_short_path(path)] = (datetime.now(), data)

  def load(self, path):
    return self.get_dict()[self.get_short_path(path)][1]

  def delete(self, path):
    del self.get_dict()[self.get_short_path(path)]

  def cleanup(self, invalidated=True, expired=True):
    key = self.checkpointer.root_path / self.checkpoint_fn.fn_subdir
    parent = key.parent
    for k, d in list(item_map.items()):
      if k == key:
        if expired and self.checkpointer.should_expire:
          for callid, (date, _) in list(d.items()):
            if self.checkpointer.should_expire(date):
              del d[callid]
      elif invalidated and k.parent == parent:
        del item_map[k]
