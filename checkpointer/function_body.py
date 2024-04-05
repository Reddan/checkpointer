import inspect
from types import FunctionType, CodeType
import relib.hashing as hashing
from pathlib import Path
from .utils import unwrap_func

cwd = Path.cwd()

def get_func_path(func):
  return Path(inspect.getfile(func)).absolute()

def get_function_body(func):
  # TODO: Strip comments
  lines = inspect.getsourcelines(func)[0]
  lines = [line.rstrip() for line in lines]
  lines = [line for line in lines if line]
  return '\n'.join(lines)

def get_code_children(__code__):
  consts = [const for const in __code__.co_consts if isinstance(const, CodeType)]
  children = [child for const in consts for child in get_code_children(const)]
  return list(__code__.co_names) + children

def get_func_children(func, neighbor_funcs=[]):
  def get_candidate_func(co_name):
    candidate_func = func.__globals__.get(co_name, None)
    return unwrap_func(candidate_func)

  def clear_candidate(candidate_func):
    return isinstance(candidate_func, FunctionType) \
      and candidate_func not in neighbor_funcs \
      and cwd in get_func_path(candidate_func).parents

  code_children = get_code_children(func.__code__)

  func_children = [get_candidate_func(child) for child in code_children]
  func_children = [child for child in func_children if clear_candidate(child)]

  funcs = [func] + [
    deep_child
    for child_func in func_children
    for deep_child in get_func_children(child_func, func_children)
  ]
  funcs = sorted(set(funcs), key=lambda func: func.__name__)

  return funcs

def get_function_hash(func):
  funcs = [func] + get_func_children(func)
  function_bodies = list(map(get_function_body, funcs))
  function_bodies_hash = hashing.hash(function_bodies)
  return function_bodies_hash
