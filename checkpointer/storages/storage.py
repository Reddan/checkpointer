from __future__ import annotations
from typing import Any, TYPE_CHECKING
from pathlib import Path
from datetime import datetime

if TYPE_CHECKING:
  from ..checkpoint import Checkpointer, CachedFunction

class Storage:
  checkpointer: Checkpointer
  cached_fn: CachedFunction

  def __init__(self, cached_fn: CachedFunction):
    self.checkpointer = cached_fn.checkpointer
    self.cached_fn = cached_fn

  def fn_id(self) -> str:
    return f"{self.cached_fn.fn_dir}/{self.cached_fn.ident.fn_hash}"

  def fn_dir(self) -> Path:
    return self.checkpointer.root_path / self.fn_id()

  def store(self, call_hash: str, data: Any) -> Any: ...

  def exists(self, call_hash: str) -> bool: ...

  def checkpoint_date(self, call_hash: str) -> datetime: ...

  def load(self, call_hash: str) -> Any: ...

  def delete(self, call_hash: str) -> None: ...

  def cleanup(self, invalidated=True, expired=True): ...
