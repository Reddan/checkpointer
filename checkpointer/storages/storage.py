from __future__ import annotations
from typing import Any, TYPE_CHECKING
from pathlib import Path
from datetime import datetime, timedelta

if TYPE_CHECKING:
  from ..checkpoint import Checkpointer, CachedFunction

class Storage:
  checkpointer: Checkpointer
  ident: CachedFunction

  def __init__(self, cached_fn: CachedFunction):
    self.checkpointer = cached_fn.ident.checkpointer
    self.cached_fn = cached_fn

  def fn_id(self) -> str:
    ident = self.cached_fn.ident
    return f"{ident.fn_dir}/{ident.fn_hash}"

  def fn_dir(self) -> Path:
    return self.checkpointer.directory / self.fn_id()

  def expired(self, call_hash: str) -> bool:
    if not self.checkpointer.expiry:
      return False
    return self.expired_dt(self.checkpoint_date(call_hash))

  def expired_dt(self, dt: datetime) -> bool:
    expiry = self.checkpointer.expiry
    if isinstance(expiry, timedelta):
      return dt < datetime.now() - expiry
    else:
      if TYPE_CHECKING: assert expiry
      return expiry(dt)

  def store(self, call_hash: str, data: Any) -> Any: ...

  def exists(self, call_hash: str) -> bool: ...

  def checkpoint_date(self, call_hash: str) -> datetime: ...

  def load(self, call_hash: str) -> Any: ...

  def delete(self, call_hash: str) -> None: ...

  def cleanup(self, invalidated=True, expired=True): ...
