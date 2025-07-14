from __future__ import annotations
import gc
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from weakref import WeakSet
from .storage import Storage

if TYPE_CHECKING:
  from ..checkpoint import CachedFunction

item_map: dict[Path, dict[str, tuple[datetime, Any]]] = {}
mem_stores: WeakSet[MemoryStorage] = WeakSet()

class MemoryStorage(Storage):
  def __init__(self, cached_fn: CachedFunction):
    super().__init__(cached_fn)
    self.cleanup()
    mem_stores.add(self)

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

  def clear(self):
    fn_path = self.fn_dir().parent
    for key in list(item_map.keys()):
      if key.parent == fn_path:
        del item_map[key]

def cleanup_memory_storage():
  gc.collect()
  storage_keys = {store.fn_dir() for store in mem_stores}
  for key in item_map.keys() - storage_keys:
    del item_map[key]
  for store in mem_stores:
    store.cleanup()
