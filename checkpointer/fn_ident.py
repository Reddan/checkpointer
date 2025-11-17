import dis
from inspect import Parameter, getmodule, signature, unwrap
from types import CodeType, MethodType, ModuleType
from typing import Annotated, Callable, Iterable, NamedTuple, Type, get_args, get_origin
from .fn_string import get_fn_aststr
from .import_mappings import resolve_annotation
from .object_hash import ObjectHash
from .types import hash_by_from_annotation, is_capture_me, is_capture_me_once, to_none
from .utils import (
  cwd, distinct, get_at, get_cell_contents,
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
    obj = get_at(self.module, *self.attr_path)
    obj = self.hash_by(obj) if self.hash_by else obj
    return self.key, obj

  @staticmethod
  def new(module: ModuleType, attr_path: AttrPath, hash_by: Callable | None, capture_once: bool) -> "Capturable":
    file = str(get_file(module).relative_to(cwd))
    key = file + "/" + ".".join(attr_path)
    cap = Capturable(key, module, attr_path, hash_by)
    if not capture_once:
      return cap
    obj_hash = str(ObjectHash(cap.capture()[1]))
    return Capturable(key, module, attr_path, None, obj_hash)

def extract_classvars(code: CodeType, scope_vars: dict) -> dict[str, dict[str, Type]]:
  attr_path = AttrPath(())
  scope_obj = None
  classvars: dict[str, dict[str, Type]] = {}
  instructs = seekable(dis.get_instructions(code))
  for instruct in instructs:
    if instruct.opname in scope_vars and not attr_path:
      attrs = takewhile((x.opname == "LOAD_ATTR", x.argval) for x in instructs)
      attr_path = AttrPath((instruct.opname, instruct.argval, *attrs))
      instructs.step(-1)
    elif instruct.opname == "CALL":
      obj = get_at(scope_vars, *attr_path)
      attr_path = AttrPath(())
      if is_class(obj):
        scope_obj = obj
    elif instruct.opname in ("STORE_FAST", "STORE_DEREF") and scope_obj:
      load_key = instruct.opname.replace("STORE", "LOAD")
      classvars.setdefault(load_key, {})[instruct.argval] = scope_obj
      scope_obj = None
  return classvars

def extract_scope_values(code: CodeType, scope_vars: dict) -> Iterable[tuple[AttrPath, object]]:
  classvars = extract_classvars(code, scope_vars)
  scope_vars = {**scope_vars, **{k: {**scope_vars[k], **v} for k, v in classvars.items()}}
  instructs = seekable(dis.get_instructions(code))
  for instruct in instructs:
    opname = instruct.opname.replace("LOAD_FAST_BORROW", "LOAD_FAST")
    if opname in scope_vars:
      attrs = takewhile((x.opname in ("LOAD_ATTR", "LOAD_METHOD"), x.argval) for x in instructs)
      attr_path = AttrPath((opname, instruct.argval, *attrs))
      parent_path = attr_path[:-1]
      instructs.step(-1)
      obj = get_at(scope_vars, *attr_path)
      if obj is not None:
        yield attr_path, obj
      if callable(obj) and parent_path[1:]:
        parent_obj = get_at(scope_vars, *parent_path)
        yield parent_path, parent_obj
  for const in code.co_consts:
    if isinstance(const, CodeType):
      next_deref = {**scope_vars["LOAD_DEREF"], **scope_vars["LOAD_FAST"]}
      next_scope_vars = {**scope_vars, "LOAD_FAST": {}, "LOAD_DEREF": next_deref}
      yield from extract_scope_values(const, next_scope_vars)

def class_from_annotation(anno: object) -> Type | None:
  if anno in (None, Annotated):
    return None
  if is_class(anno):
    return anno
  if get_origin(anno) is Annotated:
    return class_from_annotation(next(iter(get_args(anno)), None))
  return class_from_annotation(get_origin(anno))

def get_self_value(fn: Callable) -> Type | object | None:
  if isinstance(fn, MethodType):
    return fn.__self__
  parts = fn.__qualname__.split(".")[:-1]
  cls = parts and get_at(fn.__globals__, *parts)
  if is_class(cls):
    return cls

def get_capturables(fn: Callable, capture: bool, captured_vars: dict[AttrPath, object]) -> Iterable[Capturable]:
  module = getmodule(fn)
  if not module or not is_user_fn(fn):
    return
  for (instruct_type, *attr_path), obj in captured_vars.items():
    attr_path = AttrPath(attr_path)
    if instruct_type == "LOAD_GLOBAL" and not callable(obj) and not isinstance(obj, ModuleType):
      anno = resolve_annotation(module, ".".join(attr_path))
      if capture or is_capture_me(anno) or is_capture_me_once(anno):
        hash_by = hash_by_from_annotation(anno)
        if hash_by is not to_none:
          yield Capturable.new(module, attr_path, hash_by, is_capture_me_once(anno))

def get_fn_captures(fn: Callable, capture: bool) -> tuple[list[Callable], list[Capturable]]:
  scope_vars_signature: dict[str, Type | object] = {
    param.name: class_anno
    for param in signature(fn).parameters.values()
    if param.annotation is not Parameter.empty
    if param.kind not in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD)
    if (class_anno := class_from_annotation(param.annotation))
  }
  if self_obj := get_self_value(fn):
    scope_vars_signature["self"] = self_obj
  scope_vars = {
    "LOAD_FAST": scope_vars_signature,
    "LOAD_DEREF": dict(get_cell_contents(fn)),
    "LOAD_GLOBAL": fn.__globals__,
  }
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
      capturable_by_fn[depend_fn.ident.cached_fn] = []
    elif depend_fn not in capturable_by_fn and is_user_fn(depend_fn):
      get_depend_fns(depend_fn, capture, capturable_by_fn)
  return capturable_by_fn

def get_fn_ident(fn: Callable, capture: bool) -> RawFunctionIdent:
  from .checkpoint import CachedFunction
  capturable_by_fn = get_depend_fns(fn, capture)
  depends = capturable_by_fn.keys()
  depends = distinct(fn.__func__ if isinstance(fn, MethodType) else fn for fn in depends)
  depend_callables = [fn for fn in depends if not isinstance(fn, CachedFunction)]
  fn_hash = str(ObjectHash(iter=map(get_fn_aststr, depend_callables)))
  capturables = {capt for capts in capturable_by_fn.values() for capt in capts}
  assert fn == depend_callables[0]
  return RawFunctionIdent(fn_hash, depends, capturables)
