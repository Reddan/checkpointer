from contextlib import contextmanager
from typing import Any, Callable, Generic, Iterable, TypeVar, cast

T = TypeVar("T")
Fn = TypeVar("Fn", bound=Callable)

def distinct(seq: Iterable[T]) -> list[T]:
  return list(dict.fromkeys(seq))

def get_cell_contents(fn: Callable) -> Iterable[tuple[str, Any]]:
  for key, cell in zip(fn.__code__.co_freevars, fn.__closure__ or []):
    try:
      yield (key, cell.cell_contents)
    except ValueError:
      pass

def unwrap_fn(fn: Fn, cached_fn=False) -> Fn:
  from .checkpoint import CachedFunction
  while True:
    if (cached_fn and isinstance(fn, CachedFunction)) or not hasattr(fn, "__wrapped__"):
      return cast(Fn, fn)
    fn = getattr(fn, "__wrapped__")

class AwaitableValue:
  def __init__(self, value):
    self.value = value

  def __await__(self):
    yield
    return self.value

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.__dict__ = self

  def __getattribute__(self, name: str) -> Any:
    return super().__getattribute__(name)

  def __setattr__(self, name: str, value: Any) -> None:
    return super().__setattr__(name, value)

  def set(self, d: dict) -> "AttrDict":
    if not d:
      return self
    return AttrDict({**self, **d})

  def delete(self, *attrs: str) -> "AttrDict":
    d = AttrDict(self)
    for attr in attrs:
      del d[attr]
    return d

  def get_at(self, attrs: tuple[str, ...]) -> Any:
    d = self
    for attr in attrs:
      d = getattr(d, attr, None)
    return d

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

class iterate_and_upcoming(Generic[T]):
  def __init__(self, it: Iterable[T]) -> None:
    self.it = iter(it)
    self.previous: tuple[()] | tuple[T] = ()
    self.tracked = self._tracked_iter()

  def __iter__(self):
    return self

  def __next__(self) -> tuple[T, Iterable[T]]:
    try:
      item = self.previous[0] if self.previous else next(self.it)
      self.previous = ()
      return item, self.tracked
    except StopIteration:
      self.tracked.close()
      raise

  def _tracked_iter(self):
    for x in self.it:
      self.previous = (x,)
      yield x
