from typing import Generator, Coroutine, Iterable, Any, cast
from types import CellType, coroutine
from itertools import islice

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super(AttrDict, self).__init__(*args, **kwargs)
    self.__dict__ = self

  def __getattribute__(self, name: str) -> Any:
    return super().__getattribute__(name)

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

def iterate_and_upcoming[T](l: list[T]) -> Iterable[tuple[T, Iterable[T]]]:
  for i, item in enumerate(l):
    yield item, islice(l, i + 1, None)

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
