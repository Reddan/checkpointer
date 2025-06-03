import asyncio
import sys
import pytest
from pathlib import Path
from typing import Annotated, get_origin
from checkpointer import CachedFunction, Callable, CaptureMe, CaptureMeOnce, CheckpointError, HashBy, checkpoint
from .import_mappings import resolve_annotation
from .print_checkpoint import COLOR_MAP
from .utils import AttrDict

captured_dict: CaptureMe[AttrDict] = AttrDict({"a": 1, "b": 1})
captured_dict_once: CaptureMeOnce[AttrDict] = AttrDict({"a": 1, "b": 1})
nums: CaptureMe[Annotated[list[int], HashBy[sorted]]] = [3, 2, 1]

def square(x: int):
  return x * x

class TestClass:
  def __init__(self, x: int):
    self.x = x

  @checkpoint
  def add_and_square(self, y: int) -> int:
    return square(self.x + y)

test_obj: CaptureMe[TestClass] = TestClass(20)

def global_multiply(a: int, b: int) -> int:
  return a * b

def get_depend_callables(fn: CachedFunction) -> set[Callable]:
  return {
    depend
    for depend in fn.ident.deep_depends()
    if not isinstance(depend, CachedFunction) and depend != fn.fn
  }

@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
  checkpoint.root_path = Path(tmpdir)
  yield

def test_inner_capture_resolve_propagates():
  @checkpoint
  def fst():
    return captured_dict_once

  fst_capts = fst.ident.capturables
  captured_dict_once.a += 1

  @checkpoint
  def snd():
    _ = captured_dict_once
    return fst()

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

  depends1 = get_depend_callables(some_fn)
  depends2 = get_depend_callables(some_capturing_fn)
  assert depends1 == depends2 == {TestClass.add_and_square.fn, square}
  assert some_capturing_fn.ident.capturables[0].capture()[1] == test_obj

def test_decorated_method():
  result = TestClass(20).add_and_square(5)
  assert TestClass(20).add_and_square.get(5) == result

def test_resolve_annotation():
  anno = resolve_annotation(sys.modules[__name__], "COLOR_MAP")
  assert get_origin(anno) is type(COLOR_MAP)

def test_basic_caching():
  @checkpoint
  def square(x: int) -> int:
    return x ** 2

  result1 = square(4)
  result2 = square(4)

  assert result1 == result2 == 16

def test_cache_invalidation():
  @checkpoint
  def multiply(a: int, b: int):
    return a * b

  @checkpoint
  def helper(x: int):
    return multiply(x + 1, 2)

  @checkpoint
  def compute(a: int, b: int):
    return helper(a) + helper(b)

  result1 = compute(3, 4)
  assert result1 == 18

def test_layered_caching():
  dev_checkpoint = checkpoint(when=True)

  @checkpoint(format="memory")
  @dev_checkpoint
  def expensive_function(x: int):
    return x ** 2

  assert expensive_function(4) == 16
  assert expensive_function(4) == 16

def test_recursive_caching1():
  @checkpoint
  def fib(n: int) -> int:
    return fib(n - 1) + fib(n - 2) if n > 1 else n

  assert fib(10) == 55
  assert fib.get(10) == 55
  assert fib.get(5) == 5

def test_recursive_caching2():
  @checkpoint
  def fib(n: int) -> int:
    return fib.fn(n - 1) + fib.fn(n - 2) if n > 1 else n

  assert fib(10) == 55
  assert fib.get(10) == 55
  with pytest.raises(CheckpointError):
    fib.get(5)

@pytest.mark.asyncio
async def test_async_caching():
  @checkpoint(format="memory")
  async def async_square(x: int) -> int:
    await asyncio.sleep(0.1)
    return x ** 2

  result1 = await async_square(3)
  result2 = await async_square(3)
  result3 = async_square.get(3)

  assert result1 == result2 == result3 == 9

def test_force_recalculation():
  @checkpoint
  def square(x: int) -> int:
    return x ** 2

  assert square(5) == 25
  square.rerun(5)
  assert square.get(5) == 25

def test_multi_layer_decorator():
  @checkpoint(format="memory")
  @checkpoint(format="pickle")
  def add(a: int, b: int) -> int:
    return a + b

  assert add(2, 3) == 5
  assert add.get(2, 3) == 5

def test_capture():
  @checkpoint
  def test_once():
    return captured_dict_once

  @checkpoint
  def test_whole():
    return captured_dict

  @checkpoint(capture=True)
  def test_a():
    return captured_dict.a + 1

  init_hash_a = test_a.get_call_hash()
  init_hash_whole = test_whole.get_call_hash()
  captured_dict.b += 1
  assert test_whole.get_call_hash() != init_hash_whole
  assert test_a.get_call_hash() == init_hash_a
  captured_dict.a += 1
  assert test_a.get_call_hash() != init_hash_a

  once_capture = next(iter(test_once.ident.capturables))
  assert once_capture.attr_path == ("captured_dict_once",) and isinstance(once_capture.hash, str)

def test_capture_hashby():
  @checkpoint
  def fn():
    return sum(nums)

  captures = [capturable.capture() for capturable in fn.ident.capturables]
  assert nums != sorted(nums)
  assert captures == [("checkpointer/test_checkpointer.py-nums", sorted(nums))]

def test_depends():
  def multiply_wrapper(a: int, b: int) -> int:
    return global_multiply(a, b)

  def helper(a: int, b: int) -> int:
    return multiply_wrapper(a + 1, b + 1)

  @checkpoint
  def test_a(a: int, b: int) -> int:
    return helper(a, b)

  @checkpoint
  def test_b(a: int, b: int) -> int:
    return test_a(a, b) + multiply_wrapper(a, b)

  assert set(test_a.depends) == {test_a.fn, helper, multiply_wrapper, global_multiply}
  assert set(test_b.depends) == {test_b.fn, test_a, multiply_wrapper, global_multiply}

def test_lazy_init_1():
  @checkpoint
  def fn1(x: object) -> object:
    return fn2(x)

  @checkpoint
  def fn2(x: object) -> object:
    return fn1(x)

  assert set(fn1.depends) == {fn1.fn, fn2}
  assert set(fn2.depends) == {fn1, fn2.fn}

def test_lazy_init_2():
  @checkpoint
  def fn1(x: object) -> object:
    return fn2(x)

  assert set(fn1.depends) == {fn1.fn}

  @checkpoint
  def fn2(x: object) -> object:
    return fn1(x)

  assert set(fn1.depends) == {fn1.fn}
  fn1.reinit()
  assert set(fn1.depends) == {fn1.fn, fn2}
  assert set(fn2.depends) == {fn1, fn2.fn}
