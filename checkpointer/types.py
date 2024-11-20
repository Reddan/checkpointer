from __future__ import annotations
from typing import Any, TYPE_CHECKING
from pathlib import Path
from datetime import datetime

if TYPE_CHECKING:
  from .checkpoint import Checkpointer

class Storage:
  checkpointer: Checkpointer

  def __init__(self, checkpointer: Checkpointer):
    self.checkpointer = checkpointer

  def exists(self, path: Path) -> bool: ...

  def checkpoint_date(self, path: Path) -> datetime: ...

  def store(self, path: Path, data: Any) -> None: ...

  def load(self, path: Path) -> Any: ...

  def delete(self, path: Path) -> None: ...
