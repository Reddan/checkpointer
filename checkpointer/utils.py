import inspect
from typing import Generator, Coroutine, Iterable, Callable, Any, cast
from types import coroutine

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
    d = AttrDict(self.copy())
    for attr in attrs:
      del d[attr]
    return d

  def get_at(self, attrs: tuple[str, ...]) -> Any:
    d = self
    for attr in attrs:
      d = getattr(d, attr, None)
    return d

def unwrap_fn[T: Callable](fn: T, checkpoint_fn=False) -> T:
  from .checkpoint import CheckpointFn
  return inspect.unwrap(fn, stop=lambda x: checkpoint_fn and isinstance(x, CheckpointFn))

@coroutine
def coroutine_as_generator[T](coroutine: Coroutine[None, None, T]) -> Generator[None, None, T]:
  val = yield from coroutine
  return val

def sync_resolve_coroutine[T](coroutine: Coroutine[None, None, T]) -> T:
  gen = cast(Generator, coroutine_as_generator(coroutine))
  try:
    while True:
      next(gen)
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

def distinct[T](seq: Iterable[T]) -> list[T]:
  return list(dict.fromkeys(seq))

def get_cell_contents(fn: Callable) -> Generator[tuple[str, Any], None, None]:
  for key, cell in zip(fn.__code__.co_freevars, fn.__closure__ or []):
    try:
      yield (key, cell.cell_contents)
    except ValueError:
      pass
