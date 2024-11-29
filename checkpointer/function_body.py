from __future__ import annotations
import dis
import inspect
import tokenize
from io import StringIO
from collections.abc import Callable
from itertools import chain, takewhile
from operator import itemgetter
from pathlib import Path
from typing import Any, TypeGuard, TYPE_CHECKING
from types import CodeType, FunctionType
from relib import transpose, hashing, merge_dicts, drop_none
from .utils import unwrap_fn, iterate_and_upcoming, get_cell_contents, AttrDict, get_at_attr

if TYPE_CHECKING:
  from .checkpoint import CheckpointFn

cwd = Path.cwd()

def extract_scope_values(code: CodeType, scope_vars: dict[str, Any], closure = False) -> dict[tuple[str, ...], Any]:
  opname = "LOAD_GLOBAL" if not closure else "LOAD_DEREF"
  scope_values_by_path: dict[tuple[str, ...], Any] = {}
  instructions = list(dis.get_instructions(code))
  for instr, upcoming_instrs in iterate_and_upcoming(instructions):
    if instr.opname == opname:
      attrs = takewhile(lambda instr: instr.opname == "LOAD_ATTR", upcoming_instrs)
      attr_path: tuple[str, ...] = (instr.argval, *(instr.argval for instr in attrs))
      scope_values_by_path[attr_path] = get_at_attr(scope_vars, attr_path)
  children = (extract_scope_values(const, scope_vars, closure) for const in code.co_consts if isinstance(const, CodeType))
  return merge_dicts(scope_values_by_path, *children)

def get_fn_captured_vals(fn: Callable) -> list[Any]:
  closure_scope = {k: get_cell_contents(v) for k, v in zip(fn.__code__.co_freevars, fn.__closure__ or [])}
  global_vals = extract_scope_values(fn.__code__, AttrDict(fn.__globals__), closure=False)
  closure_vals = extract_scope_values(fn.__code__, AttrDict(closure_scope), closure=True)
  sorted_items = chain(sorted(global_vals.items()), sorted(closure_vals.items()))
  return drop_none(map(itemgetter(1), sorted_items))

def get_fn_body(fn: Callable) -> str:
  source = "".join(inspect.getsourcelines(fn)[0])
  tokens = tokenize.generate_tokens(StringIO(source).readline)
  ignore_types = (tokenize.COMMENT, tokenize.NL)
  return "".join("\0" + token.string for token in tokens if token.type not in ignore_types)

def get_fn_path(fn: Callable) -> Path:
  return Path(inspect.getfile(fn)).resolve()

def is_user_fn(candidate_fn) -> TypeGuard[Callable]:
  return isinstance(candidate_fn, FunctionType) \
    and cwd in get_fn_path(candidate_fn).parents

def append_fn_depends(checkpoint_fns: set[CheckpointFn], captured_vals_by_fn: dict[Callable, list[Any]], fn: Callable, capture: bool) -> None:
  from .checkpoint import CheckpointFn
  captured_vals = get_fn_captured_vals(fn)
  captured_vals_by_fn[fn] = [v for v in captured_vals if capture and not callable(v)]
  callables = [unwrap_fn(val, checkpoint_fn=True) for val in captured_vals if callable(val)]
  depends = {val for val in callables if is_user_fn(val)}
  checkpoint_fns.update({val for val in callables if isinstance(val, CheckpointFn)})
  not_appended = depends - captured_vals_by_fn.keys()
  captured_vals_by_fn.update({fn: [] for fn in not_appended})
  for child_fn in not_appended:
    append_fn_depends(checkpoint_fns, captured_vals_by_fn, child_fn, capture)

def get_depend_fns(fn: Callable, capture: bool) -> tuple[set[CheckpointFn], dict[Callable, list[Any]]]:
  checkpoint_fns: set[CheckpointFn] = set()
  captured_vals_by_fn: dict[Callable, list[Any]] = {}
  append_fn_depends(checkpoint_fns, captured_vals_by_fn, fn, capture)
  return checkpoint_fns, captured_vals_by_fn

def get_function_hash(fn: Callable, capture: bool) -> tuple[str, list[Callable]]:
  checkpoint_fns, captured_vals_by_fn = get_depend_fns(fn, capture)
  checkpoint_fns = sorted(checkpoint_fns, key=lambda fn: unwrap_fn(fn).__qualname__)
  checkpoint_hashes = [check.fn_hash for check in checkpoint_fns]
  depend_fns, depend_captured_vals = transpose(sorted(captured_vals_by_fn.items(), key=lambda x: x[0].__qualname__), 2)
  fn_bodies = list(map(get_fn_body, [fn] + depend_fns))
  fn_hash = hashing.hash((fn_bodies, depend_captured_vals, checkpoint_hashes), "blake2b")
  return fn_hash, checkpoint_fns + depend_fns
