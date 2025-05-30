from __future__ import annotations
import re
from datetime import datetime
from functools import cached_property, update_wrapper
from inspect import Parameter, Signature, iscoroutine, signature
from pathlib import Path
from typing import (
  Annotated, Callable, Concatenate, Coroutine, Generic,
  Iterable, Literal, Self, Type, TypedDict,
  Unpack, cast, get_args, get_origin, overload,
)
from .fn_ident import RawFunctionIdent, get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage
from .types import AwaitableValue, C, Coro, Fn, HashBy, P, R
from .utils import unwrap_fn

DEFAULT_DIR = Path.home() / ".cache/checkpoints"

empty_set = cast(set, frozenset())

class CheckpointError(Exception):
  pass

class CheckpointerOpts(TypedDict, total=False):
  format: Type[Storage] | Literal["pickle", "memory", "bcolz"]
  root_path: Path | str | None
  when: bool
  verbosity: Literal[0, 1, 2]
  should_expire: Callable[[datetime], bool] | None
  capture: bool
  fn_hash_from: object

class Checkpointer:
  def __init__(self, **opts: Unpack[CheckpointerOpts]):
    self.format = opts.get("format", "pickle")
    self.root_path = Path(opts.get("root_path", DEFAULT_DIR) or ".")
    self.when = opts.get("when", True)
    self.verbosity = opts.get("verbosity", 1)
    self.should_expire = opts.get("should_expire")
    self.capture = opts.get("capture", False)
    self.fn_hash_from = opts.get("fn_hash_from")

  @overload
  def __call__(self, fn: Fn, **override_opts: Unpack[CheckpointerOpts]) -> CachedFunction[Fn]: ...
  @overload
  def __call__(self, fn: None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer: ...
  def __call__(self, fn: Fn | None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer | CachedFunction[Fn]:
    if override_opts:
      opts = CheckpointerOpts(**{**self.__dict__, **override_opts})
      return Checkpointer(**opts)(fn)

    return CachedFunction(self, fn) if callable(fn) else self

class FunctionIdent:
  """
  Represents the identity and hash state of a cached function.
  Separated from CachedFunction to prevent hash desynchronization
  among bound instances when `.reinit()` is called.
  """
  def __init__(self, cached_fn: CachedFunction):
    self.__dict__.clear()
    self.cached_fn = cached_fn

  @cached_property
  def raw_ident(self) -> RawFunctionIdent:
    return get_fn_ident(unwrap_fn(self.cached_fn.fn), self.cached_fn.checkpointer.capture)

  @cached_property
  def fn_hash(self) -> str:
    if (hash_from := self.cached_fn.checkpointer.fn_hash_from) is not None:
      return str(ObjectHash(hash_from, digest_size=16))
    deep_hashes = [depend.ident.raw_ident.fn_hash for depend in self.cached_fn.deep_depends()]
    return str(ObjectHash(digest_size=16).write_text(iter=deep_hashes))

  @cached_property
  def captured_hash(self) -> str:
    deep_hashes = [depend.ident.raw_ident.captured_hash for depend in self.cached_fn.deep_depends()]
    return str(ObjectHash().write_text(iter=deep_hashes))

  def reset(self):
    self.__init__(self.cached_fn)

class CachedFunction(Generic[Fn]):
  def __init__(self, checkpointer: Checkpointer, fn: Fn):
    wrapped = unwrap_fn(fn)
    fn_file = Path(wrapped.__code__.co_filename).name
    fn_name = re.sub(r"[^\w.]", "", wrapped.__qualname__)
    Storage = STORAGE_MAP[checkpointer.format] if isinstance(checkpointer.format, str) else checkpointer.format
    update_wrapper(cast(Callable, self), wrapped)
    self.checkpointer = checkpointer
    self.fn = fn
    self.fn_dir = f"{fn_file}/{fn_name}"
    self.storage = Storage(self)
    self.cleanup = self.storage.cleanup
    self.bound = ()
    self.attrname: str | None = None

    sig = signature(wrapped)
    params = list(sig.parameters.items())
    pos_params = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    self.arg_names = [name for name, param in params if param.kind in pos_params]
    self.default_args = {name: param.default for name, param in params if param.default is not Parameter.empty}
    self.hash_by_map = get_hash_by_map(sig)
    self.ident = FunctionIdent(self)

  def __set_name__(self, _, name: str):
    assert self.attrname is None
    self.attrname = name

  @overload
  def __get__(self: Self, instance: None, owner: Type[C]) -> Self: ...
  @overload
  def __get__(self: CachedFunction[Callable[Concatenate[C, P], R]], instance: C, owner: Type[C]) -> CachedFunction[Callable[P, R]]: ...
  def __get__(self, instance, owner):
    if instance is None:
      return self
    assert self.attrname is not None
    bound_fn = object.__new__(CachedFunction)
    bound_fn.__dict__ |= self.__dict__
    bound_fn.bound = (instance,)
    if hasattr(instance, "__dict__"):
      setattr(instance, self.attrname, bound_fn)
    return bound_fn

  @property
  def depends(self) -> list[Callable]:
    return self.ident.raw_ident.depends

  def reinit(self, recursive=False) -> CachedFunction[Fn]:
    depend_idents = [depend.ident for depend in self.deep_depends()] if recursive else [self.ident]
    for ident in depend_idents: ident.reset()
    for ident in depend_idents: ident.fn_hash
    return self

  def get_call_hash(self, args: tuple, kw: dict[str, object]) -> str:
    args = self.bound + args
    pos_args = args[len(self.arg_names):]
    named_pos_args = dict(zip(self.arg_names, args))
    named_args = {**self.default_args, **named_pos_args, **kw}
    if hash_by_map := self.hash_by_map:
      rest_hash_by = hash_by_map.get(b"**")
      for key, value in named_args.items():
        if hash_by := hash_by_map.get(key, rest_hash_by):
          named_args[key] = hash_by(value)
      if pos_hash_by := hash_by_map.get(b"*"):
        pos_args = tuple(map(pos_hash_by, pos_args))
    return str(ObjectHash(named_args, pos_args, self.ident.captured_hash, digest_size=16))

  async def _resolve_coroutine(self, call_hash: str, coroutine: Coroutine):
    return self.storage.store(call_hash, AwaitableValue(await coroutine)).value

  def _call(self: CachedFunction[Callable[P, R]], args: tuple, kw: dict, rerun=False) -> R:
    full_args = self.bound + args
    params = self.checkpointer
    if not params.when:
      return self.fn(*full_args, **kw)

    call_hash = self.get_call_hash(args, kw)
    call_id = f"{self.storage.fn_id()}/{call_hash}"
    refresh = rerun \
      or not self.storage.exists(call_hash) \
      or (params.should_expire and params.should_expire(self.storage.checkpoint_date(call_hash)))

    if refresh:
      print_checkpoint(params.verbosity >= 1, "MEMORIZING", call_id, "blue")
      data = self.fn(*full_args, **kw)
      if iscoroutine(data):
        return self._resolve_coroutine(call_hash, data)
      return self.storage.store(call_hash, data)

    try:
      data = self.storage.load(call_hash)
      print_checkpoint(params.verbosity >= 2, "REMEMBERED", call_id, "green")
      return data
    except (EOFError, FileNotFoundError):
      pass
    print_checkpoint(params.verbosity >= 1, "CORRUPTED", call_id, "yellow")
    return self._call(args, kw, True)

  def __call__(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> R:
    return self._call(args, kw)

  def rerun(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> R:
    return self._call(args, kw, True)

  def exists(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> bool:
    return self.storage.exists(self.get_call_hash(args, kw))

  def delete(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs):
    self.storage.delete(self.get_call_hash(args, kw))

  @overload
  def get(self: Callable[P, Coro[R]], *args: P.args, **kw: P.kwargs) -> R: ...
  @overload
  def get(self: Callable[P, R], *args: P.args, **kw: P.kwargs) -> R: ...
  def get(self, *args, **kw):
    call_hash = self.get_call_hash(args, kw)
    try:
      data = self.storage.load(call_hash)
      return data.value if isinstance(data, AwaitableValue) else data
    except Exception as ex:
      raise CheckpointError("Could not load checkpoint") from ex

  @overload
  def set(self: Callable[P, Coro[R]], value: AwaitableValue[R], *args: P.args, **kw: P.kwargs): ...
  @overload
  def set(self: Callable[P, R], value: R, *args: P.args, **kw: P.kwargs): ...
  def set(self, value, *args, **kw):
    self.storage.store(self.get_call_hash(args, kw), value)

  def __repr__(self) -> str:
    return f"<CachedFunction {self.fn.__name__} {self.ident.fn_hash[:6]}>"

  def deep_depends(self, visited: set[CachedFunction] = empty_set) -> Iterable[CachedFunction]:
    if self not in visited:
      yield self
      visited = visited or set()
      visited.add(self)
      for depend in self.depends:
        if isinstance(depend, CachedFunction):
          yield from depend.deep_depends(visited)

def hash_by_from_annotation(annotation: type) -> Callable[[object], object] | None:
  if get_origin(annotation) is Annotated:
    args = get_args(annotation)
    metadata = args[1] if len(args) > 1 else None
    if get_origin(metadata) is HashBy:
      return get_args(metadata)[0]

def get_hash_by_map(sig: Signature) -> dict[str | bytes, Callable[[object], object]]:
  hash_by_map = {}
  for name, param in sig.parameters.items():
    if param.kind == Parameter.VAR_POSITIONAL:
      name = b"*"
    elif param.kind == Parameter.VAR_KEYWORD:
      name = b"**"
    hash_by_map[name] = hash_by_from_annotation(param.annotation)
  return hash_by_map if any(hash_by_map.values()) else {}
