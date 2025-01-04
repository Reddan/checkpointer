import asyncio
import pytest
from riprint import riprint as print
from types import MethodType, MethodWrapperType
from . import checkpoint
from .checkpoint import CheckpointError
from .utils import AttrDict

def global_multiply(a, b):
  return a * b

@pytest.fixture(autouse=True)
def run_before_and_after_tests(tmpdir):
  global checkpoint
  checkpoint = checkpoint(root_path=tmpdir)
  yield

def test_basic_caching():
  @checkpoint
  def square(x: int) -> int:
    return x ** 2

  result1 = square(4)
  result2 = square(4)

  assert result1 == result2 == 16

def test_cache_invalidation():
  @checkpoint
  def multiply(a, b):
    return a * b

  @checkpoint
  def helper(x):
    return multiply(x + 1, 2)

  @checkpoint
  def compute(a, b):
    return helper(a) + helper(b)

  result1 = compute(3, 4)
  assert result1 == 18

def test_layered_caching():
  dev_checkpoint = checkpoint(when=True)

  @checkpoint(format="memory")
  @dev_checkpoint
  def expensive_function(x):
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
  result2 = await async_square.get(3)

  assert result1 == result2 == 9

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
  def add(a, b):
    return a + b

  assert add(2, 3) == 5
  assert add.get(2, 3) == 5

def test_capture():
  item_dict = AttrDict({"a": 1, "b": 1})

  @checkpoint(capture=True)
  def test_whole():
    return item_dict

  @checkpoint(capture=True)
  def test_a():
    return item_dict.a + 1

  init_hash_a = test_a.fn_hash
  init_hash_whole = test_whole.fn_hash
  item_dict.b += 1
  test_whole.reinit()
  test_a.reinit()
  assert test_whole.fn_hash != init_hash_whole
  assert test_a.fn_hash == init_hash_a
  item_dict.a += 1
  test_a.reinit()
  assert test_a.fn_hash != init_hash_a

def test_depends():
  def multiply_wrapper(a, b):
    return global_multiply(a, b)

  def helper(a, b):
    return multiply_wrapper(a + 1, b + 1)

  @checkpoint
  def test_a(a, b):
    return helper(a, b)

  @checkpoint
  def test_b(a, b):
    return test_a(a, b) + multiply_wrapper(a, b)

  assert set(test_a.depends) == {test_a.fn, helper, multiply_wrapper, global_multiply}
  assert set(test_b.depends) == {test_b.fn, test_a, multiply_wrapper, global_multiply}

def test_lazy_init():
  @checkpoint
  def fn1(x):
    return fn2(x)

  @checkpoint
  def fn2(x):
    return fn1(x)

  assert type(object.__getattribute__(fn1, "_getattribute")) == MethodType
  with pytest.raises(AttributeError):
    object.__getattribute__(fn1, "fn_hash")
  assert fn1.fn_hash == object.__getattribute__(fn1, "fn_hash")
  assert type(object.__getattribute__(fn1, "_getattribute")) == MethodWrapperType
  assert set(fn1.depends) == {fn1.fn, fn2}
  assert set(fn2.depends) == {fn1, fn2.fn}
