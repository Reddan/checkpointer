import inspect
from types import FunctionType, CodeType
from relib.raypipe import raypipe
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
  return raypipe \
    .filter(lambda const: isinstance(const, CodeType)) \
    .flat_map(get_code_children) \
    .do(lambda children: list(__code__.co_names) + children) \
    .compute(__code__.co_consts)

def get_func_children(func, neighbor_funcs=[]):
  def get_candidate_func(co_name):
    candidate_func = func.__globals__.get(co_name, None)
    return unwrap_func(candidate_func)

  def clear_candidate(candidate_func):
    return isinstance(candidate_func, FunctionType) \
      and candidate_func not in neighbor_funcs \
      and cwd in get_func_path(candidate_func).parents

  code_children = get_code_children(func.__code__)

  func_children = raypipe \
    .map(get_candidate_func) \
    .filter(clear_candidate) \
    .compute(code_children)

  funcs = raypipe \
    .flat_map(lambda child_func: \
      get_func_children(child_func, func_children)
    ) \
    .do(lambda grand_children: [func] + grand_children) \
    .sort_distinct(lambda func: func.__name__) \
    .compute(func_children)

  return funcs

def get_function_hash(func):
  funcs = [func] + get_func_children(func)
  function_bodies = raypipe.map(get_function_body).compute(funcs)
  function_bodies_hash = hashing.hash(function_bodies)
  return function_bodies_hash
