import inspect
import relib.hashing as hashing
from collections.abc import Callable
from types import FunctionType, CodeType
from pathlib import Path
from .utils import unwrap_fn

cwd = Path.cwd()

def get_fn_path(fn: Callable) -> Path:
  return Path(inspect.getfile(fn)).resolve()

def get_function_body(fn: Callable) -> str:
  # TODO: Strip comments
  lines = inspect.getsourcelines(fn)[0]
  lines = [line.rstrip() for line in lines]
  lines = [line for line in lines if line]
  return "\n".join(lines)

def get_code_children(code: CodeType) -> list[str]:
  consts = [const for const in code.co_consts if isinstance(const, CodeType)]
  children = [child for const in consts for child in get_code_children(const)]
  return list(code.co_names) + children

def is_user_fn(candidate_fn, cleared_fns: set[Callable]) -> bool:
  return isinstance(candidate_fn, FunctionType) \
    and candidate_fn not in cleared_fns \
    and cwd in get_fn_path(candidate_fn).parents

def append_fn_children(cleared_fns: set[Callable], fn: Callable) -> None:
  code_children = get_code_children(fn.__code__)
  fn_children = [unwrap_fn(fn.__globals__.get(co_name, None)) for co_name in code_children]
  fn_children = [child for child in fn_children if is_user_fn(child, cleared_fns)]
  cleared_fns.update(fn_children)
  for child_fn in fn_children:
    append_fn_children(cleared_fns, child_fn)

def get_fn_children(fn: Callable) -> list[Callable]:
  cleared_fns: set[Callable] = set()
  append_fn_children(cleared_fns, fn)
  return sorted(cleared_fns, key=lambda fn: fn.__name__)

def get_function_hash(fn: Callable) -> str:
  fns = [fn] + get_fn_children(fn)
  fn_bodies = list(map(get_function_body, fns))
  return hashing.hash(fn_bodies)
