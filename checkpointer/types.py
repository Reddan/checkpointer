from typing import Annotated, Callable, Coroutine, Generic, ParamSpec, TypeVar

Fn = TypeVar("Fn", bound=Callable)
P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")
T = TypeVar("T")

class HashBy(Generic[Fn]):
  pass

NoHash = Annotated[T, HashBy[lambda _: None]]
Coro = Coroutine[object, object, R]

class AwaitableValue(Generic[T]):
  def __init__(self, value: T):
    self.value = value

  def __await__(self):
    yield
    return self.value
