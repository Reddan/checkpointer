import asyncio
import sys
import pytest
from functools import wraps
from pathlib import Path
from typing import Annotated, get_origin
from checkpointer import CachedFunction, Callable, CaptureMe, CaptureMeOnce, Checkpointer, CheckpointError, HashBy, checkpoint
from .import_mappings import resolve_annotation
from .print_checkpoint import COLOR_MAP
from .utils import AttrDict

captured_dict: CaptureMe[AttrDict] = AttrDict({"a": 1, "b": 1})
captured_dict_once: CaptureMeOnce[AttrDict] = AttrDict({"a": 1, "b": 1})
nums: CaptureMe[Annotated[list[int], HashBy[sorted]]] = [3, 2, 1]

def get_captured_objs(fn: CachedFunction) -> dict[str, object]:
  captures = [capturable.capture() for capturable in fn.ident.capturables]
  return {key.rsplit("/", 1)[1]: value for key, value in captures}

def get_depends(fn: CachedFunction) -> set[Callable]:
  return set(fn.ident.raw_ident.depends)

def get_deep_callables(fn: CachedFunction) -> set[Callable]:
  return {
    depend
    for depend in fn.ident.deep_depends()
    if not isinstance(depend, CachedFunction) and depend != fn.fn
  }

def square(x: int):
  return x * x

def multiply(x: int, y: int) -> int:
  return x * y

class TestClass:
  def __init__(self, x: int):
    self.x = x

  def square(self, x: int):
    return square(x)

  @checkpoint
  def add_and_square(self, y: int) -> int:
    return self.square(self.x + y)

test_obj: CaptureMe[TestClass] = TestClass(20)

@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
  checkpoint.directory = Path(tmpdir)
  yield

def test_same_and_different_fn_hash():
  @checkpoint
  def foo1(a: int, /, b: int, c: int = 0, *args: int, d: int, e: int = 0, **kwargs: int):
    """Documentation for foo"""

    return ([
      a, b, c,  #
      args, d, e,
      "",
    ])

  @checkpoint
  def foo2(a, b, c, *args, d, e, **kwargs): return [a, b, c, args, d, e, '']
  @checkpoint
  def bar(a, b, c, *args, d, e, **kwargs): return [a, b, c, args, d, '']
  @checkpoint
  def baz(a, b, c, *args, d, e, **kw): return [a, b, c, args, d, e, '']

  assert foo1.ident.fn_hash == foo2.ident.fn_hash
  assert foo2.ident.fn_hash != bar.ident.fn_hash
  assert foo2.ident.fn_hash != baz.ident.fn_hash

def test_cache_invalidation():
  def square_1(x: int): return x * x + 0
  def square_2(x: int): return x * x + 0
  def square_3(x: int): return 0 + x * x
  @checkpoint
  def helper(x: int): square(x)
  @checkpoint
  def fn(x: int): helper(x)

  square = square_1
  init_fn_hash = fn.ident.fn_hash
  square = square_2
  assert init_fn_hash == fn.reinit().ident.fn_hash
  square = square_3
  assert init_fn_hash != fn.reinit().ident.fn_hash

def test_inner_capture_resolve_propagates():
  @checkpoint
  def fst(): return captured_dict_once

  fst_capts = fst.ident.capturables
  captured_dict_once.a += 1

  @checkpoint
  def snd(): return captured_dict_once | fst()

  snd_capts = snd.ident.capturables
  assert fst_capts == snd_capts

def test_method_capturing():
  @checkpoint
  def some_fn():
    obj = TestClass(10)
    return obj.add_and_square(5)

  @checkpoint
  def some_capturing_fn():
    return test_obj.add_and_square(5)

  @checkpoint
  def some_fn_arg(obj: TestClass):
    def closure():
      return obj.add_and_square(5)
    return closure()

  target_depends = {TestClass.add_and_square.fn, TestClass.square, square}
  assert get_deep_callables(some_fn) == target_depends
  assert get_deep_callables(some_capturing_fn) == target_depends
  assert get_deep_callables(some_fn_arg) == target_depends
  assert get_captured_objs(some_capturing_fn) == {"test_obj": test_obj}

def test_decorated_method():
  a = TestClass(10).add_and_square(5)
  b = TestClass(5).add_and_square(5)
  assert (a, b) == (225, 100)
  assert TestClass(5).add_and_square.get(5) == b
  assert TestClass.add_and_square.get(TestClass(5), 5) == b
  assert TestClass.add_and_square(TestClass(5), 5) == b

def test_resolve_annotation():
  anno = resolve_annotation(sys.modules[__name__], "COLOR_MAP")
  assert get_origin(anno) is type(COLOR_MAP)

def test_layered_caching():
  dev_checkpoint = checkpoint(when=True)

  @checkpoint(storage="memory")
  @dev_checkpoint
  def expensive_function(x: int): return x ** 2

  assert expensive_function(4) == 16
  assert expensive_function.get(4) == 16
  assert expensive_function.fn.get(4) == 16

def test_recursive_caching():
  @checkpoint
  def fib1(n: int) -> int:
    return fib1(n - 1) + fib1(n - 2) if n > 1 else n

  @checkpoint
  def fib2(n: int) -> int:
    return fib2.fn(n - 1) + fib2.fn(n - 2) if n > 1 else n

  assert (fib1(10), fib1.get(10), fib1.get(5)) == (55, 55, 5)
  assert (fib2(10), fib2.get(10)) == (55, 55)
  with pytest.raises(CheckpointError):
    fib2.get(5)

@pytest.mark.asyncio
async def test_async_caching():
  @checkpoint(storage="memory")
  async def async_square(x: int) -> int:
    await asyncio.sleep(0.1)
    return x ** 2

  results = (await async_square(3), await async_square(3), async_square.get(3), async_square.get(3))
  assert results == (9, 9, 9, 9)

def test_force_recalculation():
  @checkpoint
  def square(x: int) -> int: return x ** 2 + offset

  offset = 0
  assert square(5) == 25
  offset = 10
  assert square.rerun(5) == 35
  offset = 20
  assert square(5) == 35

def test_multi_layer_decorator():
  @checkpoint(storage="memory")
  @checkpoint(storage="pickle")
  def add(a: int, b: int) -> int: return a + b

  assert add(2, 3) == 5
  assert add.get(2, 3) == 5
  assert add.fn.fn

def test_capture():
  @checkpoint
  def test_once(): return captured_dict_once
  @checkpoint
  def test_whole(): return captured_dict
  @checkpoint(capture=True)
  def test_a(): return captured_dict.a + 1

  init_hash_a = test_a.get_call_hash()
  init_hash_whole = test_whole.get_call_hash()
  captured_dict.b += 1
  assert test_whole.get_call_hash() != init_hash_whole
  assert test_a.get_call_hash() == init_hash_a
  captured_dict.a += 1
  assert test_a.get_call_hash() != init_hash_a

  once_capture = next(iter(test_once.ident.capturables))
  assert isinstance(once_capture.hash, str)
  assert once_capture.hash == once_capture.capture()[1]
  assert once_capture.attr_path == ("captured_dict_once",)

def test_hashby():
  @checkpoint
  def fn(
    pos: float,
    *args: float,
    **kwargs: int,
  ): ...

  @checkpoint
  def fn_hashby(
    pos: Annotated[float, HashBy[float.__floor__]],
    *args: Annotated[float, HashBy[float.__ceil__]],
    **kwargs: Annotated[int, HashBy[lambda x: x % 10]],
  ): ...

  pairs = [
    (fn_hashby.get_call_hash(1.1, 1.1, x=1), fn.get_call_hash(1, 2, x=1)),
    (fn_hashby.get_call_hash(1.9, 1.9, x=11), fn.get_call_hash(1, 2, x=1)),
    (fn_hashby.get_call_hash(2.1, 1.1, x=1), fn.get_call_hash(2, 2, x=1)),
    (fn_hashby.get_call_hash(1.1, 2.1, x=1), fn.get_call_hash(1, 3, x=1)),
    (fn_hashby.get_call_hash(1.1, 1.1, x=2), fn.get_call_hash(1, 2, x=2)),
  ]
  assert all(a == b for a, b in pairs)
  assert len({b for _, b in pairs}) == 4

def test_capture_hashby():
  @checkpoint
  def fn():
    return sum(nums)

  assert nums != sorted(nums)
  assert get_captured_objs(fn) == {"nums": sorted(nums)}

def test_depends():
  def multiply_wrapper(a: int, b: int): return multiply(a, b)
  def helper(a: int, b: int): return multiply_wrapper(a + 1, b + 1)
  @checkpoint
  def test_a(a: int, b: int): return helper(a, b)
  @checkpoint
  def test_b(a: int, b: int): return test_a(a, b) + multiply_wrapper(a, b)

  assert get_depends(test_a) == {test_a.fn, helper, multiply_wrapper, multiply}
  assert get_depends(test_b) == {test_b.fn, test_a, multiply_wrapper, multiply}

def test_lazy_init():
  for ident_early in [True, False]:
    @checkpoint
    def fn1(x: object): return fn2(x)

    if ident_early:
      assert get_depends(fn1) == {fn1.fn}

    @checkpoint
    def fn2(x: object): return fn1(x)

    if ident_early:
      assert get_depends(fn1) == {fn1.fn}
      fn1.reinit()
    assert get_depends(fn1) == {fn1.fn, fn2}
    assert get_depends(fn2) == {fn1, fn2.fn}

def test_repr():
  @checkpoint
  def fn(): ...

  assert str(fn) == "<CachedFunction fn - uninitialized>"
  assert str(fn) == "<CachedFunction fn - uninitialized>"
  fn()
  assert str(fn) == f"<CachedFunction fn {fn.ident.fn_hash[:6]}>"

def test_ignore_checkpoint_decorator():
  def included_decorator(fn: Callable) -> Callable:
    @wraps(fn)
    def wrapper(*args, **kwargs):
      return fn(*args, **kwargs)
    return wrapper

  @checkpoint
  def fn_a(): ...

  @checkpoint
  @included_decorator
  def fn_b(): ...

  @checkpoint
  @checkpoint
  @included_decorator
  @Checkpointer()
  def fn_c(): ...

  assert fn_a.ident.fn_hash != fn_b.ident.fn_hash
  assert fn_b.ident.fn_hash == fn_c.ident.fn_hash
