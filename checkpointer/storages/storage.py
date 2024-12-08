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

  def store(self, path: Path, data: Any) -> None: ...

  def exists(self, path: Path) -> bool: ...

  def checkpoint_date(self, path: Path) -> datetime: ...

  def load(self, path: Path) -> Any: ...

  def delete(self, path: Path) -> None: ...

  def cleanup(self, invalidated=True, expired=True): ...
