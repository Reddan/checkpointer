from typing import Any
from pathlib import Path
from datetime import datetime
from .storage import Storage

item_map: dict[Path, dict[str, tuple[datetime, Any]]] = {}

class MemoryStorage(Storage):
  def get_dict(self):
    return item_map.setdefault(self.fn_dir(), {})

  def store(self, call_hash, data):
    self.get_dict()[call_hash] = (datetime.now(), data)
    return data

  def exists(self, call_hash):
    return call_hash in self.get_dict()

  def checkpoint_date(self, call_hash):
    return self.get_dict()[call_hash][0]

  def load(self, call_hash):
    return self.get_dict()[call_hash][1]

  def delete(self, call_hash):
    self.get_dict().pop(call_hash, None)

  def cleanup(self, invalidated=True, expired=True):
    curr_key = self.fn_dir()
    for key, calldict in list(item_map.items()):
      if key.parent == curr_key.parent:
        if invalidated and key != curr_key:
          del item_map[key]
        elif expired and self.checkpointer.expiry:
          for call_hash, (date, _) in list(calldict.items()):
            if self.expired_dt(date):
              del calldict[call_hash]
