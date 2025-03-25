import inspect
import tokenize
from contextlib import contextmanager
from io import StringIO
from types import coroutine
from typing import Any, Callable, Coroutine, Generator, Generic, Iterable, TypeVar, cast

T = TypeVar("T")
T_Callable = TypeVar("T_Callable", bound=Callable)

def distinct(seq: Iterable[T]) -> list[T]:
  return list(dict.fromkeys(seq))

def transpose(tuples, default_num_returns=0):
  output = tuple(zip(*tuples))
  if not output:
    return ([],) * default_num_returns
  return tuple(map(list, output))

def get_fn_body(fn: Callable) -> str:
  try:
    source = inspect.getsource(fn)
  except OSError:
    return ""
  tokens = tokenize.generate_tokens(StringIO(source).readline)
  ignore_types = (tokenize.COMMENT, tokenize.NL)
  return "".join("\0" + token.string for token in tokens if token.type not in ignore_types)

def get_cell_contents(fn: Callable) -> Iterable[tuple[str, Any]]:
  for key, cell in zip(fn.__code__.co_freevars, fn.__closure__ or []):
    try:
      yield (key, cell.cell_contents)
    except ValueError:
      pass

def unwrap_fn(fn: T_Callable, checkpoint_fn=False) -> T_Callable:
  from .checkpoint import CheckpointFn
  while True:
    if (checkpoint_fn and isinstance(fn, CheckpointFn)) or not hasattr(fn, "__wrapped__"):
      return cast(T_Callable, fn)
    fn = getattr(fn, "__wrapped__")

async def resolved_awaitable(value: T) -> T:
  return value

@coroutine
def coroutine_as_generator(coroutine: Coroutine[None, None, T]) -> Generator[None, None, T]:
  val = yield from coroutine
  return val

def sync_resolve_coroutine(coroutine: Coroutine[None, None, T]) -> T:
  gen = cast(Generator, coroutine_as_generator(coroutine))
  try:
    while True:
      next(gen)
  except StopIteration as ex:
    return ex.value

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
