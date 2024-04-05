import types

def unwrap_func(func):
  if hasattr(func, '__wrapped__'):
    return unwrap_func(func.__wrapped__)
  else:
    return func

@types.coroutine
def coroutine_as_generator(coroutine):
  val = yield from coroutine
  return val

def sync_resolve_coroutine(coroutine):
  try:
    next(coroutine_as_generator(coroutine))
  except StopIteration as ex:
    return ex.value
