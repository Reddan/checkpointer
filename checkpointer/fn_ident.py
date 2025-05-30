import dis
import inspect
from itertools import takewhile
from pathlib import Path
from types import CodeType, FunctionType, MethodType
from typing import Callable, Iterable, NamedTuple, Type, TypeGuard
from .object_hash import ObjectHash
from .utils import AttrDict, distinct, get_cell_contents, iterate_and_upcoming, unwrap_fn

cwd = Path.cwd().resolve()

class RawFunctionIdent(NamedTuple):
  fn_hash: str
  captured_hash: str
  depends: list[Callable]

def is_class(obj) -> TypeGuard[Type]:
  # isinstance works too, but needlessly triggers _lazyinit()
  return issubclass(type(obj), type)

def extract_classvars(code: CodeType, scope_vars: AttrDict) -> dict[str, dict[str, Type]]:
  attr_path: tuple[str, ...] = ()
  scope_obj = None
  classvars: dict[str, dict[str, Type]] = {}
  for instr, upcoming_instrs in iterate_and_upcoming(dis.get_instructions(code)):
    if instr.opname in scope_vars and not attr_path:
      attrs = takewhile(lambda instr: instr.opname == "LOAD_ATTR", upcoming_instrs)
      attr_path = (instr.opname, instr.argval, *(str(x.argval) for x in attrs))
    elif instr.opname == "CALL":
      obj = scope_vars.get_at(attr_path)
      attr_path = ()
      if is_class(obj):
        scope_obj = obj
    elif instr.opname in ("STORE_FAST", "STORE_DEREF") and scope_obj:
      load_key = instr.opname.replace("STORE", "LOAD")
      classvars.setdefault(load_key, {})[instr.argval] = scope_obj
      scope_obj = None
  return classvars

def extract_scope_values(code: CodeType, scope_vars: AttrDict) -> Iterable[tuple[tuple[str, ...], object]]:
  classvars = extract_classvars(code, scope_vars)
  scope_vars = scope_vars.set({k: scope_vars[k].set(v) for k, v in classvars.items()})
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

def get_fn_captured_vals(fn: Callable) -> list[object]:
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

def get_depend_fns(fn: Callable, captured_vals_by_fn: dict[Callable, list[object]] = {}) -> dict[Callable, list[object]]:
  from .checkpoint import CachedFunction
  captured_vals = get_fn_captured_vals(fn)
  captured_vals_by_fn = captured_vals_by_fn or {}
  captured_vals_by_fn[fn] = [val for val in captured_vals if not callable(val)]
  for val in captured_vals:
    if not callable(val):
      continue
    child_fn = unwrap_fn(val, cached_fn=True)
    if isinstance(child_fn, CachedFunction):
      captured_vals_by_fn[child_fn] = []
    elif child_fn not in captured_vals_by_fn and is_user_fn(child_fn):
      get_depend_fns(child_fn, captured_vals_by_fn)
  return captured_vals_by_fn

def get_fn_ident(fn: Callable, capture: bool) -> RawFunctionIdent:
  from .checkpoint import CachedFunction
  captured_vals_by_fn = get_depend_fns(fn)
  depend_captured_vals = list(captured_vals_by_fn.values()) * capture
  depends = captured_vals_by_fn.keys()
  depends = distinct(fn.__func__ if isinstance(fn, MethodType) else fn for fn in depends)
  unwrapped_depends = [fn for fn in depends if not isinstance(fn, CachedFunction)]
  assert fn == unwrapped_depends[0]
  fn_hash = str(ObjectHash(iter=unwrapped_depends))
  captured_hash = str(ObjectHash(iter=depend_captured_vals, tolerate_errors=True))
  return RawFunctionIdent(fn_hash, captured_hash, depends)
