import dis
from inspect import unwrap
from types import CodeType, MethodType
from typing import Callable, Iterable, NamedTuple, Type
from .object_hash import ObjectHash
from .utils import AttrDict, distinct, get_cell_contents, is_class, is_user_fn, seekable, takewhile

AttrPath = tuple[str, ...]

class RawFunctionIdent(NamedTuple):
  fn_hash: str
  captured_hash: str
  depends: list[Callable]

def extract_classvars(code: CodeType, scope_vars: AttrDict) -> dict[str, dict[str, Type]]:
  attr_path = AttrPath(())
  scope_obj = None
  classvars: dict[str, dict[str, Type]] = {}
  instructs = seekable(dis.get_instructions(code))
  for instr in instructs:
    if instr.opname in scope_vars and not attr_path:
      attrs = takewhile((x.opname == "LOAD_ATTR", x.argval) for x in instructs)
      attr_path = AttrPath((instr.opname, instr.argval, *attrs))
      instructs.step(-1)
    elif instr.opname == "CALL":
      obj = scope_vars.get_at(*attr_path)
      attr_path = AttrPath(())
      if is_class(obj):
        scope_obj = obj
    elif instr.opname in ("STORE_FAST", "STORE_DEREF") and scope_obj:
      load_key = instr.opname.replace("STORE", "LOAD")
      classvars.setdefault(load_key, {})[instr.argval] = scope_obj
      scope_obj = None
  return classvars

def extract_scope_values(code: CodeType, scope_vars: AttrDict) -> Iterable[tuple[AttrPath, object]]:
  classvars = extract_classvars(code, scope_vars)
  scope_vars = scope_vars.set({k: scope_vars[k].set(v) for k, v in classvars.items()})
  instructs = seekable(dis.get_instructions(code))
  for instr in instructs:
    if instr.opname in scope_vars:
      attrs = takewhile((x.opname in ("LOAD_ATTR", "LOAD_METHOD"), x.argval) for x in instructs)
      attr_path = AttrPath((instr.opname, instr.argval, *attrs))
      parent_path = attr_path[:-1]
      instructs.step(-1)
      obj = scope_vars.get_at(*attr_path)
      if obj is not None:
        yield attr_path, obj
      if callable(obj) and parent_path[1:]:
        parent_obj = scope_vars.get_at(*parent_path)
        yield parent_path, parent_obj
  for const in code.co_consts:
    if isinstance(const, CodeType):
      yield from extract_scope_values(const, scope_vars)

def get_self_value(fn: Callable) -> type | object | None:
  if isinstance(fn, MethodType):
    return fn.__self__
  parts = fn.__qualname__.split(".")[:-1]
  cls = parts and AttrDict(fn.__globals__).get_at(*parts)
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

def get_depend_fns(fn: Callable, captured_vals_by_fn: dict[Callable, list[object]] = {}) -> dict[Callable, list[object]]:
  from .checkpoint import CachedFunction
  captured_vals = get_fn_captured_vals(fn)
  captured_vals_by_fn = captured_vals_by_fn or {}
  captured_vals_by_fn[fn] = [val for val in captured_vals if not callable(val)]
  for val in captured_vals:
    if not callable(val):
      continue
    child_fn = unwrap(val, stop=lambda f: isinstance(f, CachedFunction))
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
