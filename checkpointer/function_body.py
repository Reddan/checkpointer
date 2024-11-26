import dis
import inspect
import relib.hashing as hashing
from collections.abc import Callable
from typing import TypeGuard
from types import FunctionType, CodeType
from pathlib import Path
from .utils import unwrap_fn

cwd = Path.cwd()

def get_cell_contents(cell):
  try:
    return cell.cell_contents
  except ValueError:
    return None

def get_fn_path(fn: Callable) -> Path:
  return Path(inspect.getfile(fn)).resolve()

def get_fn_body(fn: Callable) -> str:
  # TODO: Strip comments
  lines = inspect.getsourcelines(fn)[0]
  lines = [line.rstrip() for line in lines]
  lines = [line for line in lines if line]
  return "\n".join(lines)

def is_user_fn(candidate_fn) -> TypeGuard[FunctionType]:
  return isinstance(candidate_fn, FunctionType) \
    and cwd in get_fn_path(candidate_fn).parents

def get_referenced_global_names(code: CodeType) -> set[str]:
  variables = {instr.argval for instr in dis.get_instructions(code) if instr.opname == "LOAD_GLOBAL"}
  children = [get_referenced_global_names(const) for const in code.co_consts if isinstance(const, CodeType)]
  return variables.union(*children)

def get_global_depends(fn: Callable) -> set[Callable]:
  vardict = fn.__globals__
  co_names = get_referenced_global_names(fn.__code__)
  reference_vals = [unwrap_fn(vardict.get(co_name)) for co_name in co_names]
  return {val for val in reference_vals if is_user_fn(val)}

def get_closure_depends(fn: Callable) -> set[Callable]:
  cell_vals = [unwrap_fn(get_cell_contents(cell)) for cell in fn.__closure__ or []]
  return {val for val in cell_vals if is_user_fn(val)}

def append_fn_depends(cleared_fns: set[Callable], fn: Callable) -> None:
  depends = get_global_depends(fn) | get_closure_depends(fn)
  not_cleared = depends - cleared_fns
  cleared_fns.update(not_cleared)
  for child_fn in not_cleared:
    append_fn_depends(cleared_fns, child_fn)

def get_depend_fns(fn: Callable) -> list[Callable]:
  cleared_fns: set[Callable] = set()
  append_fn_depends(cleared_fns, fn)
  return sorted(cleared_fns, key=lambda fn: fn.__qualname__)

def get_function_hash(fn: Callable) -> tuple[str, list[Callable]]:
  depends = get_depend_fns(fn)
  fn_bodies = list(map(get_fn_body, [fn] + depends))
  fn_hash = hashing.hash(fn_bodies)
  return fn_hash, depends
