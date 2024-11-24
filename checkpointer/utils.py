from typing import Generator, Coroutine, cast
from types import coroutine

def unwrap_fn[T](fn: T) -> T:
  while hasattr(fn, "__wrapped__"):
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
