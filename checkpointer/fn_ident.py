from __future__ import annotations
import dis
import inspect
import tokenize
from collections.abc import Callable
from io import StringIO
from itertools import takewhile
from pathlib import Path
from types import CodeType, FunctionType
from typing import TYPE_CHECKING, Any, Generator, TypeGuard
from relib import hashing
from .utils import AttrDict, get_cell_contents, iterate_and_upcoming, transpose, unwrap_fn

if TYPE_CHECKING:
  from .checkpoint import CheckpointFn

cwd = Path.cwd()

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

def get_fn_captured_vals(fn: Callable) -> list[Any]:
  scope_vars = AttrDict({
    "LOAD_DEREF": AttrDict(get_cell_contents(fn)),
    "LOAD_GLOBAL": AttrDict(fn.__globals__),
  })
  vals = dict(extract_scope_values(fn.__code__, scope_vars))
  return list(vals.values())

def get_fn_body(fn: Callable) -> str:
  source = "".join(inspect.getsourcelines(fn)[0])
  tokens = tokenize.generate_tokens(StringIO(source).readline)
  ignore_types = (tokenize.COMMENT, tokenize.NL)
  return "".join("\0" + token.string for token in tokens if token.type not in ignore_types)

def is_user_fn(candidate_fn) -> TypeGuard[Callable]:
  if not isinstance(candidate_fn, FunctionType):
    return False
  fn_path = Path(inspect.getfile(candidate_fn)).resolve()
  return cwd in fn_path.parents and ".venv" not in fn_path.parts

def append_fn_depends(checkpoint_fns: set[CheckpointFn], captured_vals_by_fn: dict[Callable, list[Any]], fn: Callable, capture: bool) -> None:
  from .checkpoint import CheckpointFn
  captured_vals = get_fn_captured_vals(fn)
  captured_vals_by_fn[fn] = [val for val in captured_vals if capture and not callable(val)]
  callables = [unwrap_fn(val, checkpoint_fn=True) for val in captured_vals if callable(val)]
  checkpoint_fns.update(val for val in callables if isinstance(val, CheckpointFn))
  depends = {val for val in callables if is_user_fn(val)}
  not_appended = depends - captured_vals_by_fn.keys()
  captured_vals_by_fn.update({fn: [] for fn in not_appended})
  for child_fn in not_appended:
    append_fn_depends(checkpoint_fns, captured_vals_by_fn, child_fn, capture)

def get_depend_fns(fn: Callable, capture: bool) -> tuple[set[CheckpointFn], dict[Callable, list[Any]]]:
  checkpoint_fns: set[CheckpointFn] = set()
  captured_vals_by_fn: dict[Callable, list[Any]] = {}
  append_fn_depends(checkpoint_fns, captured_vals_by_fn, fn, capture)
  return checkpoint_fns, captured_vals_by_fn

def get_fn_ident(fn: Callable, capture: bool) -> tuple[str, list[Callable]]:
  checkpoint_fns, captured_vals_by_fn = get_depend_fns(fn, capture)
  checkpoint_fns = sorted(checkpoint_fns, key=lambda fn: unwrap_fn(fn).__qualname__)
  checkpoint_hashes = [check.fn_hash for check in checkpoint_fns]
  depend_fns, depend_captured_vals = transpose(sorted(captured_vals_by_fn.items(), key=lambda x: x[0].__qualname__), 2)
  fn_bodies = list(map(get_fn_body, [fn] + depend_fns))
  fn_hash = hashing.hash((fn_bodies, depend_captured_vals, checkpoint_hashes), "blake2b")
  return fn_hash, checkpoint_fns + depend_fns

def get_function_hash(fn: Callable, capture=False) -> str:
  return get_fn_ident(fn, capture)[0]
