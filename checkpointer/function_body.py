import dis
import inspect
import relib.hashing as hashing
from itertools import takewhile, islice, chain
from collections.abc import Callable
from typing import Literal, TypeGuard, Any
from types import FunctionType, CodeType
from pathlib import Path
from .utils import unwrap_fn

cwd = Path.cwd()

def get_at_attr(d: dict, keys: tuple[str, ...]) -> Any:
  try:
    d = d[keys[0]]
    for key in keys[1:]:
      d = getattr(d, key)
  except KeyError:
    return ...
  return d

def get_cell_contents(cell) -> Any:
  try:
    return cell.cell_contents
  except ValueError:
    return ...

def get_referenced_names(code: CodeType, closure = False) -> set[tuple[str, ...]]:
  opname = "LOAD_GLOBAL" if not closure else "LOAD_DEREF"
  variables: set[tuple[str, ...]] = set()
  instructions = list(dis.get_instructions(code))

  for i, instr in enumerate(instructions):
    if instr.opname == opname:
      name = instr.argval
      attrs = takewhile(lambda instr: instr.opname == "LOAD_ATTR", islice(instructions, i + 1, None))
      attr_path = (name, *(instr.argval for instr in attrs))
      variables.add(attr_path)

  children = [get_referenced_names(const, closure) for const in code.co_consts if isinstance(const, CodeType)]

  return variables.union(*children)

def get_fn_accessmap(fn: Callable) -> dict[Literal["global", "closure"], dict[tuple[str, ...], Any]]:
  global_vars = fn.__globals__
  closure_vars = {k: get_cell_contents(v) for k, v in zip(fn.__code__.co_freevars, fn.__closure__ or [])}
  global_names = get_referenced_names(fn.__code__, closure=False)
  closure_names = get_referenced_names(fn.__code__, closure=True)
  global_dict = {name: get_at_attr(global_vars, name) for name in sorted(global_names)}
  closure_dict = {name: get_at_attr(closure_vars, name) for name in sorted(closure_names)}
  return {"global": global_dict, "closure": closure_dict}

def get_fn_path(fn: Callable) -> Path:
  return Path(inspect.getfile(fn)).resolve()

def get_fn_body(fn: Callable) -> str:
  # TODO: Strip comments
  lines = inspect.getsourcelines(fn)[0]
  lines = [line.rstrip() for line in lines]
  lines = [line for line in lines if line]
  return "\n".join(lines)

def is_user_fn(candidate_fn) -> TypeGuard[Callable]:
  return isinstance(candidate_fn, FunctionType) \
    and cwd in get_fn_path(candidate_fn).parents

def append_fn_depends(cleared_fns: set[Callable], fn: Callable) -> None:
  accessmap = get_fn_accessmap(fn)
  vals = (unwrap_fn(val) for val in chain(accessmap["global"].values(), accessmap["closure"].values()))
  depends = {val for val in vals if is_user_fn(val)}
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
