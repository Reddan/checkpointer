import inspect
from types import FunctionType, CodeType
import relib.hashing as hashing
from pathlib import Path
from .utils import unwrap_func

cwd = Path.cwd()

def get_fn_path(fn):
  return Path(inspect.getfile(fn)).absolute()

def get_function_body(fn):
  # TODO: Strip comments
  lines = inspect.getsourcelines(fn)[0]
  lines = [line.rstrip() for line in lines]
  lines = [line for line in lines if line]
  return '\n'.join(lines)

def get_code_children(__code__):
  consts = [const for const in __code__.co_consts if isinstance(const, CodeType)]
  children = [child for const in consts for child in get_code_children(const)]
  return list(__code__.co_names) + children

def is_user_fn(candidate_fn, cleared_fns):
  return isinstance(candidate_fn, FunctionType) \
    and candidate_fn not in cleared_fns \
    and cwd in get_fn_path(candidate_fn).parents

def append_fn_children(fn, cleared_fns):
  code_children = get_code_children(fn.__code__)
  fn_children = [unwrap_func(fn.__globals__.get(co_name, None)) for co_name in code_children]
  fn_children = [child for child in fn_children if is_user_fn(child, cleared_fns)]

  for fn in fn_children:
    cleared_fns.add(fn)

  for child_fn in fn_children:
    append_fn_children(child_fn, cleared_fns)

def get_fn_children(fn):
  cleared_fns = set()
  append_fn_children(fn, cleared_fns)
  return sorted(cleared_fns, key=lambda fn: fn.__name__)

def get_function_hash(fn):
  fns = [fn] + get_fn_children(fn)
  fn_bodies = list(map(get_function_body, fns))
  fn_bodies_hash = hashing.hash(fn_bodies)
  return fn_bodies_hash
