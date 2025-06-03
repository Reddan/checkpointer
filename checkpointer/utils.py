from __future__ import annotations
import inspect
from contextlib import contextmanager
from itertools import islice
from pathlib import Path
from types import FunctionType, MethodType, ModuleType
from typing import Callable, Generic, Iterable, Self, Type, TypeGuard
from .types import T

cwd = Path.cwd().resolve()

def is_class(obj) -> TypeGuard[Type]:
  return isinstance(obj, type)

def get_file(obj: Callable | ModuleType) -> Path:
  return Path(inspect.getfile(obj)).resolve()

def is_user_file(path: Path) -> bool:
  return cwd in path.parents and ".venv" not in path.parts

def is_user_fn(obj) -> TypeGuard[Callable]:
  return isinstance(obj, (FunctionType, MethodType)) and is_user_file(get_file(obj))

def get_cell_contents(fn: Callable) -> Iterable[tuple[str, object]]:
  for key, cell in zip(fn.__code__.co_freevars, fn.__closure__ or []):
    try:
      yield (key, cell.cell_contents)
    except ValueError:
      pass

def distinct(seq: Iterable[T]) -> list[T]:
  return list(dict.fromkeys(seq))

def takewhile(iter: Iterable[tuple[bool, T]]) -> Iterable[T]:
  for condition, value in iter:
    if not condition:
      return
    yield value

class seekable(Generic[T]):
  def __init__(self, iterable: Iterable[T]):
    self.index = 0
    self.source = iter(iterable)
    self.sink: list[T] = []

  def __iter__(self):
    return self

  def __next__(self) -> T:
    if len(self.sink) > self.index:
      item = self.sink[self.index]
    else:
      item = next(self.source)
      self.sink.append(item)
    self.index += 1
    return item

  def __bool__(self):
    return bool(self.lookahead(1))

  def seek(self, index: int) -> Self:
    remainder = index - len(self.sink)
    if remainder > 0:
      next(islice(self, remainder, remainder), None)
    self.index = max(0, min(index, len(self.sink)))
    return self

  def step(self, count: int) -> Self:
    return self.seek(self.index + count)

  @contextmanager
  def freeze(self):
    initial_index = self.index
    try:
      yield
    finally:
      self.seek(initial_index)

  def lookahead(self, count: int) -> list[T]:
    with self.freeze():
      return list(islice(self, count))

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.__dict__ = self

  def __getattribute__(self, name: str):
    return super().__getattribute__(name)

  def __setattr__(self, name: str, value: object):
    super().__setattr__(name, value)

  def set(self, d: dict) -> AttrDict:
    if not d:
      return self
    return AttrDict({**self, **d})

  def get_at(self: object, *attrs: str) -> object:
    obj = self
    for attr in attrs:
      obj = getattr(obj, attr, None)
    return obj

class ContextVar(Generic[T]):
  def __init__(self, value: T):
    self.value = value

  @contextmanager
  def set(self, value: T):
    self.value, old = value, self.value
    try:
      yield
    finally:
      self.value = old
