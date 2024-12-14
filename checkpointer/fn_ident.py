from __future__ import annotations
import dis
import inspect
from collections.abc import Callable
from itertools import takewhile
from pathlib import Path
from types import CodeType, FunctionType, MethodType
from typing import TYPE_CHECKING, Any, Generator, TypeGuard
from .object_hash import ObjectHash
from .utils import AttrDict, distinct, get_cell_contents, iterate_and_upcoming, transpose, unwrap_fn

if TYPE_CHECKING:
  from .checkpoint import CheckpointFn

cwd = Path.cwd()

def is_class(obj):
  return isinstance(obj, type)

def extract_scope_values(code: CodeType, scope_vars: AttrDict) -> Generator[tuple[tuple[str, ...], Any], None, None]:
  for instr, upcoming_instrs in iterate_and_upcoming(dis.get_instructions(code)):
    if instr.opname in scope_vars:
      attrs = takewhile(lambda instr: instr.opname == "LOAD_ATTR", upcoming_instrs)
      attr_path: tuple[str, ...] = (instr.opname, instr.argval, *(str(x.argval) for x in attrs))
      val = scope_vars.get_at(attr_path)
      if val is not None:
        yield attr_path, val
  for const in code.co_consts:
    if isinstance(const, CodeType):
      yield from extract_scope_values(const, scope_vars)

def get_self_value(fn: Callable) -> type | object | None:
  if isinstance(fn, MethodType):
    return fn.__self__
  parts = tuple(fn.__qualname__.split(".")[:-1])
  cls = parts and AttrDict(fn.__globals__).get_at(parts)
  if is_class(cls):
    return cls

def get_fn_captured_vals(fn: Callable) -> list[Any]:
  self_value = get_self_value(fn)
  scope_vars = AttrDict({
    "LOAD_FAST": AttrDict({"self": self_value} if self_value else {}),
    "LOAD_DEREF": AttrDict(get_cell_contents(fn)),
    "LOAD_GLOBAL": AttrDict(fn.__globals__),
  })
  vals = dict(extract_scope_values(fn.__code__, scope_vars))
  return list(vals.values())

def is_user_fn(candidate_fn) -> TypeGuard[Callable]:
  if not isinstance(candidate_fn, (FunctionType, MethodType)):
    return False
  fn_path = Path(inspect.getfile(candidate_fn)).resolve()
  return cwd in fn_path.parents and ".venv" not in fn_path.parts

def append_fn_depends(checkpoint_fns: dict[CheckpointFn, None], captured_vals_by_fn: dict[Callable, list[Any]], fn: Callable, capture: bool) -> None:
  from .checkpoint import CheckpointFn
  captured_vals = get_fn_captured_vals(fn)
  captured_vals_by_fn[fn] = [val for val in captured_vals if not callable(val)] * capture
  child_fns = (unwrap_fn(val, checkpoint_fn=True) for val in captured_vals if callable(val))
  for child_fn in child_fns:
    if isinstance(child_fn, CheckpointFn):
      checkpoint_fns[child_fn] = None
    elif child_fn not in captured_vals_by_fn and is_user_fn(child_fn):
      append_fn_depends(checkpoint_fns, captured_vals_by_fn, child_fn, capture)

def get_depend_fns(fn: Callable, capture: bool) -> tuple[list[CheckpointFn], dict[Callable, list[Any]]]:
  checkpoint_fns: dict[CheckpointFn, None] = {}
  captured_vals_by_fn: dict[Callable, list[Any]] = {}
  append_fn_depends(checkpoint_fns, captured_vals_by_fn, fn, capture)
  return list(checkpoint_fns), captured_vals_by_fn

def get_fn_ident(fn: Callable, capture: bool) -> tuple[str, list[Callable]]:
  checkpoint_fns, captured_vals_by_fn = get_depend_fns(fn, capture)
  checkpoint_hashes = [check.fn_hash for check in checkpoint_fns]
  depend_fns, depend_captured_vals = transpose(captured_vals_by_fn.items(), 2)
  depend_fns = distinct(fn.__func__ if isinstance(fn, MethodType) else fn for fn in depend_fns)
  fn_hash = ObjectHash(fn, depend_fns, checkpoint_hashes).update(depend_captured_vals, tolerate_errors=True)
  return str(fn_hash), checkpoint_fns + depend_fns

def get_function_hash(fn: Callable, capture=False) -> str:
  return get_fn_ident(fn, capture)[0]
