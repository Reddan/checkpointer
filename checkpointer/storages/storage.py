from __future__ import annotations
from typing import Any, TYPE_CHECKING
from pathlib import Path
from datetime import datetime

if TYPE_CHECKING:
  from ..checkpoint import Checkpointer, CheckpointFn

class Storage:
  checkpointer: Checkpointer
  checkpoint_fn: CheckpointFn

  def __init__(self, checkpoint_fn: CheckpointFn):
    self.checkpointer = checkpoint_fn.checkpointer
    self.checkpoint_fn = checkpoint_fn

  def dir(self) -> Path:
    return self.checkpointer.root_path / self.checkpoint_fn.fn_dir / self.checkpoint_fn.fn_hash

  def store(self, call_id: str, data: Any) -> None: ...

  def exists(self, call_id: str) -> bool: ...

  def checkpoint_date(self, call_id: str) -> datetime: ...

  def load(self, call_id: str) -> Any: ...

  def delete(self, call_id: str) -> None: ...

  def cleanup(self, invalidated=True, expired=True): ...
