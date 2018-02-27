import os
import inspect
from types import FunctionType, CodeType
from relib import imports, hashing, raypipe

def get_function_dir_path(func):
  return os.path.dirname(os.path.abspath(func.__code__.co_filename))

def get_function_project_dir_path(func_dir_path):
  return imports.find_parent_dir_containing(func_dir_path, '.git') or imports.find_parent_dir_containing(func_dir_path, '__init__.py') or os.getcwd()

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

def get_func_children(func, func_proj_path, func_by_wrapper={}, neighbor_funcs=[]):
  def get_candidate_func(co_name):
    try:
      candidate_func = func.__globals__.get(co_name, None)
      return func_by_wrapper.get(candidate_func, candidate_func)
    except TypeError:
      # non hashable datatype
      return None

  def clear_candidate(candidate_func):
    if isinstance(candidate_func, FunctionType):
      if candidate_func not in neighbor_funcs:
        func_dir_path = get_function_dir_path(candidate_func)
        return func_proj_path in func_dir_path
    return False

  code_children = get_code_children(func.__code__)

  func_children = raypipe \
    .map(get_candidate_func) \
    .filter(clear_candidate) \
    .compute(code_children)

  funcs = raypipe \
    .flat_map(lambda child_func: get_func_children(child_func, func_proj_path, func_by_wrapper, func_children)) \
    .do(lambda grand_children: [func] + grand_children) \
    .sort_distinct(lambda func: func.__name__) \
    .compute(func_children)

  return funcs

def get_function_hash(func, func_by_wrapper={}):
  func_dir_path = get_function_dir_path(func)
  func_proj_path = get_function_project_dir_path(func_dir_path)
  funcs = [func] + get_func_children(func, func_proj_path, func_by_wrapper)
  function_bodies = raypipe.map(get_function_body).compute(funcs)
  function_bodies_hash = hashing.hash(function_bodies)
  return function_bodies_hash
