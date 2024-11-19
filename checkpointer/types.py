from typing import Protocol, Any
from pathlib import Path
from datetime import datetime

class Storage(Protocol):
  @staticmethod
  def exists(path: Path) -> bool: ...

  @staticmethod
  def checkpoint_date(path: Path) -> datetime: ...

  @staticmethod
  def store(path: Path, data: Any) -> None: ...

  @staticmethod
  def load(path: Path) -> Any: ...

  @staticmethod
  def delete(path: Path) -> None: ...
