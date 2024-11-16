from typing import Callable, Protocol, Any
from pathlib import Path
from datetime import datetime

class Storage(Protocol):
  @staticmethod
  def is_expired(path: Path) -> bool: ...

  @staticmethod
  def should_expire(path: Path, expire_fn: Callable[[datetime], bool]) -> bool: ...

  @staticmethod
  def store_data(path: Path, data: Any) -> Any: ...

  @staticmethod
  def load_data(path: Path) -> Any: ...

  @staticmethod
  def delete_data(path: Path) -> None: ...
