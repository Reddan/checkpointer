from typing import Generator, Coroutine, Iterable, Any, cast
from types import CellType, coroutine

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super(AttrDict, self).__init__(*args, **kwargs)
    self.__dict__ = self

  def __getattribute__(self, name: str) -> Any:
    return super().__getattribute__(name)

  def __setattr__(self, name: str, value: Any) -> None:
    return super().__setattr__(name, value)

def unwrap_fn[T](fn: T, checkpoint_fn=False) -> T:
  from .checkpoint import CheckpointFn
  while hasattr(fn, "__wrapped__"):
    if checkpoint_fn and isinstance(fn, CheckpointFn):
      return fn
    fn = getattr(fn, "__wrapped__")
  return fn

@coroutine
def coroutine_as_generator[T](coroutine: Coroutine[None, None, T]) -> Generator[None, None, T]:
  val = yield from coroutine
  return val

def sync_resolve_coroutine[T](coroutine: Coroutine[None, None, T]) -> T:
  gen = cast(Generator, coroutine_as_generator(coroutine))
  try:
    while True: next(gen)
  except StopIteration as ex:
    return ex.value

async def resolved_awaitable[T](value: T) -> T:
  return value

class iterate_and_upcoming[T]:
  def __init__(self, it: Iterable[T]) -> None:
    self.it = iter(it)
    self.previous: tuple[()] | tuple[T] = ()

  def __iter__(self):
    return self

  def __next__(self) -> tuple[T, Iterable[T]]:
    item = self.previous[0] if self.previous else next(self.it)
    self.previous = ()
    return item, self._tracked_iter()

  def _tracked_iter(self):
    for x in self.it:
      self.previous = (x,)
      yield x

def get_at_attr(d: dict, keys: tuple[str, ...]) -> Any:
  try:
    for key in keys:
      d = getattr(d, key)
  except AttributeError:
    return None
  return d

def get_cell_contents(cell: CellType) -> Any:
  try:
    return cell.cell_contents
  except ValueError:
    return None
