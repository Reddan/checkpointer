from __future__ import annotations
from relib import clear_console, hashing
clear_console()
import inspect
import dis
from icecream import ic
from io import StringIO
from . import checkpoint
from . import function_body
from .function_body import is_user_fn, get_fn_body
from typing import Any
import tokenize

hashing.hash(...)

checkpoint = checkpoint(capture=True)

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super(AttrDict, self).__init__(*args, **kwargs)
    self.__dict__ = self

  def __getattribute__(self, name: str) -> Any:
    return super().__getattribute__(name)



item_dict = AttrDict({"a": 101, "b": 2052})

def test(x: int):
  def inner(y: int):
    def innerest(z: int):
      return x * y * z
    return innerest
  return inner

func1 = test(2)(3)
func2 = test(2)(3)
func3 = test(4)(3)

# cp1 = checkpoint(func1)
# cp3 = checkpoint(func3)
# print(cp1)
# print(cp1.fn_hash)
# print(cp3)
# print(cp3.fn_hash)

class Yeller:
  def __init__(self, value: str, child: Yeller | None = None):
    self.value = value
    self.child = child or self

  def yell(self):
    print(self.value.upper())


global_yeller = Yeller("heeeya")

@checkpoint
def wrong_order(a, b):
  if a + b == 7:
    return wrong_order(a + 1, b + 1)
  return wrong_order_depend(a, b)

def wrong_order_depend(a, b):
  # NOTE: changes here doesn't invalidate wrong_order
  return a * b

def way_removed(x):
  return -x + 5 # + len(item_dict)

def out_multiply(a, b):
  return way_removed(a * b)

# @checkpoint
def out_helper(x):
  return out_multiply(x + 1, 2)

@checkpoint
def out_compute(a, b):
  global_yeller.yell()
  def innerfn(x):
    return x + 1
  x = item_dict.a + innerfn(2)
  return out_helper(a) + out_helper(b) + x

print(out_compute(3, 4))


def main(xarg: int):
  class CloseYeller:
    def __init__(self, value: str, child: Yeller | None = None):
      self.value = value
      self.child = child or self

    def yell(self):
      print(self.value.upper())

  print("\n\nMain()")
  yeller = Yeller("hello")
  close_yeller = CloseYeller("hello", yeller)

  @checkpoint
  def helper(x):
    def inner(yeller, CloseYeller, compute, out_multiply):
      def innerest():
        x = (yeller, CloseYeller, compute, out_multiply)
        ...
    yeller.yell()
    closer_yeller = CloseYeller("hello")
    closer_yeller.child.yell()
    if x == 1:
      return compute(3, 4)
    return out_multiply(x + 1, 2)

  @checkpoint
  def compute(a, b):
    function_body.get_depend_fns(helper.fn, False)
    close_yeller.child.child.yell()
    if a == 10:
      return compute(3, 4)
    def inner():
      return helper(a) + helper(b)
    return inner()

  print(compute(3, 4))
  print(compute(1, 5))


main(10)







def create_closure1(x):
  def closure_depend(y):
    way_removed(y)
    return x + y

  def closure_fn(y):
    return x + y + closure_depend(y)
  return closure_fn



fnc1_global = create_closure1(2)
def create_closure2(x):
  fnc1_local = create_closure1(2)
  def closure_depend(y):
    way_removed(y)
    return x + y

  def closure_fn(y):
    return x + y + closure_depend(y) + fnc1_local(y)
  return closure_fn

fnc2 = checkpoint(create_closure2(2), capture=False)
fnc2(3)
assert {fn.__qualname__ for fn in fnc2.depends} == {"create_closure1.<locals>.closure_depend", "create_closure1.<locals>.closure_fn", "create_closure2.<locals>.closure_depend", "create_closure2.<locals>.closure_fn", "way_removed"}
