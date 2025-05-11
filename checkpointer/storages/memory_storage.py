from typing import Any
from pathlib import Path
from datetime import datetime
from .storage import Storage

item_map: dict[Path, dict[str, tuple[datetime, Any]]] = {}

class MemoryStorage(Storage):
  def get_dict(self):
    return item_map.setdefault(self.fn_dir(), {})

  def store(self, call_id, data):
    self.get_dict()[call_id] = (datetime.now(), data)

  def exists(self, call_id):
    return call_id in self.get_dict()

  def checkpoint_date(self, call_id):
    return self.get_dict()[call_id][0]

  def load(self, call_id):
    return self.get_dict()[call_id][1]

  def delete(self, call_id):
    self.get_dict().pop(call_id, None)

  def cleanup(self, invalidated=True, expired=True):
    curr_key = self.fn_dir()
    for key, calldict in list(item_map.items()):
      if key.parent == curr_key.parent:
        if invalidated and key != curr_key:
          del item_map[key]
        elif expired and self.checkpointer.should_expire:
          for call_id, (date, _) in list(calldict.items()):
            if self.checkpointer.should_expire(date):
              del calldict[call_id]
