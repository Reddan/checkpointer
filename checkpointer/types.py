from typing import Annotated, Callable, Generic, TypeVar

T = TypeVar("T")
Fn = TypeVar("Fn", bound=Callable)

class HashBy(Generic[Fn]):
  pass

NoHash = Annotated[T, HashBy[lambda _: None]]

class AwaitableValue:
  def __init__(self, value):
    self.value = value

  def __await__(self):
    yield
    return self.value
