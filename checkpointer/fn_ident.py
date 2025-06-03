import dis
from inspect import getmodule, unwrap
from types import CodeType, MethodType, ModuleType
from typing import Callable, Iterable, NamedTuple, Type
from .import_mappings import resolve_annotation
from .object_hash import ObjectHash
from .types import hash_by_from_annotation, is_capture_me, is_capture_me_once, to_none
from .utils import (
  AttrDict, cwd, distinct, get_cell_contents,
  get_file, is_class, is_user_fn, seekable, takewhile,
)

AttrPath = tuple[str, ...]
CapturableByFn = dict[Callable, list["Capturable"]]

class RawFunctionIdent(NamedTuple):
  fn_hash: str
  depends: list[Callable]
  capturables: set["Capturable"]

class Capturable(NamedTuple):
  key: str
  module: ModuleType
  attr_path: AttrPath
  hash_by: Callable | None
  hash: str | None = None

  def capture(self) -> tuple[str, object]:
    if obj := self.hash:
      return self.key, obj
    obj = AttrDict.get_at(self.module, *self.attr_path)
    obj = self.hash_by(obj) if self.hash_by else obj
    return self.key, obj

  @staticmethod
  def new(module: ModuleType, attr_path: AttrPath, hash_by: Callable | None, capture_once: bool) -> "Capturable":
    file = str(get_file(module).relative_to(cwd))
    key = "-".join((file, *attr_path))
    cap = Capturable(key, module, attr_path, hash_by)
    if not capture_once:
      return cap
    obj_hash = str(ObjectHash(cap.capture()[1]))
    return Capturable(key, module, attr_path, None, obj_hash)

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

def get_capturables(fn: Callable, capture: bool, captured_vars: dict[AttrPath, object]) -> Iterable[Capturable]:
  module = getmodule(fn)
  if not module or not is_user_fn(fn):
    return
  for (instruct, *attr_path), obj in captured_vars.items():
    attr_path = AttrPath(attr_path)
    if instruct == "LOAD_GLOBAL" and not callable(obj) and not isinstance(obj, ModuleType):
      anno = resolve_annotation(module, ".".join(attr_path))
      if capture or is_capture_me(anno) or is_capture_me_once(anno):
        hash_by = hash_by_from_annotation(anno)
        if hash_by is not to_none:
          yield Capturable.new(module, attr_path, hash_by, is_capture_me_once(anno))

def get_fn_captures(fn: Callable, capture: bool) -> tuple[list[Callable], list[Capturable]]:
  self_value = get_self_value(fn)
  scope_vars = AttrDict({
    "LOAD_FAST": AttrDict({"self": self_value} if self_value else {}),
    "LOAD_DEREF": AttrDict(get_cell_contents(fn)),
    "LOAD_GLOBAL": AttrDict(fn.__globals__),
  })
  captured_vars = dict(extract_scope_values(fn.__code__, scope_vars))
  captured_callables = [obj for obj in captured_vars.values() if callable(obj)]
  capturables = list(get_capturables(fn, capture, captured_vars))
  return captured_callables, capturables

def get_depend_fns(fn: Callable, capture: bool, capturable_by_fn: CapturableByFn = {}) -> CapturableByFn:
  from .checkpoint import CachedFunction
  captured_callables, capturables = get_fn_captures(fn, capture)
  capturable_by_fn = capturable_by_fn or {}
  capturable_by_fn[fn] = capturables
  for depend_fn in captured_callables:
    depend_fn = unwrap(depend_fn, stop=lambda f: isinstance(f, CachedFunction))
    if isinstance(depend_fn, CachedFunction):
      capturable_by_fn[depend_fn] = []
    elif depend_fn not in capturable_by_fn and is_user_fn(depend_fn):
      get_depend_fns(depend_fn, capture, capturable_by_fn)
  return capturable_by_fn

def get_fn_ident(fn: Callable, capture: bool) -> RawFunctionIdent:
  from .checkpoint import CachedFunction
  capturable_by_fn = get_depend_fns(fn, capture)
  capturables = {capt for capts in capturable_by_fn.values() for capt in capts}
  depends = capturable_by_fn.keys()
  depends = distinct(fn.__func__ if isinstance(fn, MethodType) else fn for fn in depends)
  unwrapped_depends = [fn for fn in depends if not isinstance(fn, CachedFunction)]
  assert fn == unwrapped_depends[0]
  fn_hash = str(ObjectHash(iter=unwrapped_depends))
  return RawFunctionIdent(fn_hash, depends, capturables)
