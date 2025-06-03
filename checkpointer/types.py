from typing import (
  Annotated, Callable, Coroutine, Generic,
  ParamSpec, TypeVar, get_args, get_origin,
)

Fn = TypeVar("Fn", bound=Callable)
P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")
T = TypeVar("T")

class HashBy(Generic[Fn]):
  pass

class Captured:
  pass

class CapturedOnce:
  pass

def to_none(_):
  return None

def get_annotated_args(anno: object) -> tuple[object, ...]:
  return get_args(anno) if get_origin(anno) is Annotated else ()

def hash_by_from_annotation(anno: object) -> Callable[[object], object] | None:
  for arg in get_annotated_args(anno):
    if get_origin(arg) is HashBy:
      return get_args(arg)[0]

def is_capture_me(anno: object) -> bool:
  return Captured in get_annotated_args(anno)

def is_capture_me_once(anno: object) -> bool:
  return CapturedOnce in get_annotated_args(anno)

NoHash = Annotated[T, HashBy[to_none]]
CaptureMe = Annotated[T, Captured]
CaptureMeOnce = Annotated[T, CapturedOnce]
Coro = Coroutine[object, object, R]

class AwaitableValue(Generic[T]):
  def __init__(self, value: T):
    self.value = value

  def __await__(self):
    yield
    return self.value
