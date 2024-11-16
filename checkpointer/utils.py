import types

def unwrap_fn[T](fn: T) -> T:
  while hasattr(fn, "__wrapped__"):
    fn = getattr(fn, "__wrapped__")
  return fn

@types.coroutine
def coroutine_as_generator(coroutine):
  val = yield from coroutine
  return val

def sync_resolve_coroutine(coroutine):
  try:
    next(coroutine_as_generator(coroutine))
  except StopIteration as ex:
    return ex.value
