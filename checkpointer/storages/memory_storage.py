from typing import Any
from pathlib import Path
from datetime import datetime
from ..types import Storage

item_map: dict[str, tuple[datetime, Any]] = {}

class MemoryStorage(Storage):
  def get_short_path(self, path: Path):
    return str(path.relative_to(self.checkpointer.root_path))

  def exists(self, path):
    return self.get_short_path(path) in item_map

  def checkpoint_date(self, path):
    return item_map[self.get_short_path(path)][0]

  def store(self, path, data):
    item_map[self.get_short_path(path)] = (datetime.now(), data)

  def load(self, path):
    return item_map[self.get_short_path(path)][1]

  def delete(self, path):
    del item_map[self.get_short_path(path)]
