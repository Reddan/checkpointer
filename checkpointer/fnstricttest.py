from relib import clear_console
clear_console()
from . import checkpoint
from .utils import AttrDict

print(AttrDict([("b", 234), ("c", 123)], x=1337))

item_dict = AttrDict({"a": 1021, "b": 213152})

def global_multiply(a, b):
  return a * b

def helper(a, b):
  return global_multiply(a + 1, b + 1)

@checkpoint
def chill_fn(a, b):
  print("item_dict", item_dict)
  return helper(a, b)

@checkpoint(capture=True)
def strict_fn(a, b):
  print("item_dict", item_dict)
  return helper(a, b)

@checkpoint
def chill_on_strict(a, b):
  # `helper` is not in chill_on_strict.depends, but `multiply` forcefully is
  return strict_fn(a, b) + global_multiply(a, b)

print(chill_fn(3, 4))
print(chill_fn.depends)
print(strict_fn(3, 4))
print(strict_fn.depends)
print(chill_on_strict(3, 4))
print(chill_on_strict.depends)

print("---------------------------------")
