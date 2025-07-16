from __future__ import annotations
import re
from datetime import datetime, timedelta
from functools import cached_property, update_wrapper
from inspect import Parameter, iscoroutine, signature, unwrap
from pathlib import Path
from typing import (
  Callable, Concatenate, Coroutine, Generic, Iterable,
  Literal, Self, Type, TypedDict, Unpack, overload,
)
from .fn_ident import Capturable, RawFunctionIdent, get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage, StorageType
from .types import AwaitableValue, C, Coro, Fn, P, R, T, hash_by_from_annotation
from .utils import flatten, to_coroutine

DEFAULT_DIR = Path.home() / ".cache/checkpoints"

class CheckpointError(Exception):
  pass

class CheckpointerOpts(TypedDict, total=False):
  storage: Type[Storage] | StorageType
  directory: Path | str | None
  when: bool
  verbosity: Literal[0, 1, 2]
  expiry: timedelta | Callable[[datetime], bool] | None
  capture: bool
  fn_hash_from: object

class Checkpointer:
  def __init__(self, **opts: Unpack[CheckpointerOpts]):
    self.storage = opts.get("storage", "pickle")
    self.directory = Path(opts.get("directory", DEFAULT_DIR) or ".")
    self.when = opts.get("when", True)
    self.verbosity = opts.get("verbosity", 1)
    self.expiry = opts.get("expiry")
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
  __slots__ = (
    "checkpointer", "cached_fn", "fn", "fn_dir", "pos_names",
    "arg_names", "default_args", "hash_by_map", "__dict__",
  )

  def __init__(self, cached_fn: CachedFunction, checkpointer: Checkpointer, fn: Callable):
    wrapped = unwrap(fn)
    fn_file = Path(wrapped.__code__.co_filename).name
    fn_name = re.sub(r"[^\w.]", "", wrapped.__qualname__)
    params = list(signature(wrapped).parameters.values())
    pos_param_types = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    named_param_types = (Parameter.KEYWORD_ONLY,) + pos_param_types
    name_by_kind = {Parameter.VAR_POSITIONAL: b"*", Parameter.VAR_KEYWORD: b"**"}
    self.checkpointer = checkpointer
    self.cached_fn = cached_fn
    self.fn = fn
    self.fn_dir = f"{fn_file}/{fn_name}"
    self.pos_names = [param.name for param in params if param.kind in pos_param_types]
    self.arg_names = {param.name for param in params if param.kind in named_param_types}
    self.default_args = {param.name: param.default for param in params if param.default is not Parameter.empty}
    self.hash_by_map = {
      name_by_kind.get(param.kind, param.name): hash_by
      for param in params
      if (hash_by := hash_by_from_annotation(param.annotation))
    }

  def reset(self):
    self.__dict__.clear()

  def is_static(self) -> bool:
    return self.checkpointer.fn_hash_from is not None

  @cached_property
  def raw_ident(self) -> RawFunctionIdent:
    return get_fn_ident(unwrap(self.fn), self.checkpointer.capture)

  @cached_property
  def fn_hash(self) -> str:
    if self.is_static():
      return str(ObjectHash(self.checkpointer.fn_hash_from, digest_size=16))
    depends = self.deep_idents(past_static=False)
    deep_hashes = [d.fn_hash if d.is_static() else d.raw_ident.fn_hash for d in depends]
    return str(ObjectHash(digest_size=16).write_text(iter=deep_hashes))

  @cached_property
  def capturables(self) -> list[Capturable]:
    return sorted({
      capturable.key: capturable
      for depend in self.deep_idents()
      for capturable in depend.raw_ident.capturables
    }.values())

  def deep_depends(self, past_static=True, visited: set[Callable] = set()) -> Iterable[Callable]:
    if self.cached_fn not in visited:
      yield self.cached_fn
      visited = visited or set()
      visited.add(self.cached_fn)
      stop = not past_static and self.is_static()
      depends = [] if stop else self.raw_ident.depends
      for depend in depends:
        if isinstance(depend, CachedFunction):
          yield from depend.ident.deep_depends(past_static, visited)
        elif depend not in visited:
          yield depend
          visited.add(depend)

  def deep_idents(self, past_static=True) -> Iterable[FunctionIdent]:
    return (fn.ident for fn in self.deep_depends(past_static) if isinstance(fn, CachedFunction))

class CachedFunction(Generic[Fn]):
  def __init__(self, checkpointer: Checkpointer, fn: Fn):
    update_wrapper(self, unwrap(fn))  # type: ignore
    self.ident = FunctionIdent(self, checkpointer, fn)
    self.bound = ()

  @overload
  def __get__(self: Self, instance: None, owner: Type[C]) -> Self: ...
  @overload
  def __get__(
    self: CachedFunction[Callable[Concatenate[C, P], R]],
    instance: C,
    owner: Type[C],
  ) -> CachedFunction[Callable[P, R]]: ...
  def __get__(self, instance, owner):
    if instance is None:
      return self
    bound_fn = object.__new__(CachedFunction)
    bound_fn.__dict__ |= self.__dict__
    bound_fn.bound = (instance,)
    return bound_fn

  @property
  def fn(self) -> Fn:
    return self.ident.fn  # type: ignore

  @cached_property
  def storage(self) -> Storage:
    store_format = self.ident.checkpointer.storage
    Storage = STORAGE_MAP[store_format] if isinstance(store_format, str) else store_format
    return Storage(self)

  @property
  def cleanup(self):
    return self.storage.cleanup

  def reinit(self, recursive=True) -> CachedFunction[Fn]:
    depend_idents = list(self.ident.deep_idents()) if recursive else [self.ident]
    for ident in depend_idents: ident.reset()
    for ident in depend_idents: ident.fn_hash
    return self

  def _get_call_hash(self, args: tuple, kw: dict[str, object]) -> str:
    ident = self.ident
    args = self.bound + args
    pos_args = args[len(ident.pos_names):]
    named_pos_args = dict(zip(ident.pos_names, args))
    named_args = {**ident.default_args, **named_pos_args, **kw}
    for key, hash_by in ident.hash_by_map.items():
      if isinstance(key, str):
        named_args[key] = hash_by(named_args[key])
      elif key == b"*":
        pos_args = map(hash_by, pos_args)
      elif key == b"**":
        for key in kw.keys() - ident.arg_names:
          named_args[key] = hash_by(named_args[key])
    call_hash = ObjectHash(digest_size=16) \
      .update(header="NAMED", iter=flatten(sorted(named_args.items()))) \
      .update(header="POS", iter=pos_args) \
      .update(header="CAPTURED", iter=flatten(c.capture() for c in ident.capturables))
    return str(call_hash)

  def get_call_hash(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> str:
    return self._get_call_hash(args, kw)

  async def _store_coroutine(self, call_hash: str, coroutine: Coroutine):
    return self.storage.store(call_hash, AwaitableValue(await coroutine)).value

  def _call(self: CachedFunction[Callable[P, R]], args: tuple, kw: dict, rerun=False) -> R:
    full_args = self.bound + args
    params = self.ident.checkpointer
    storage = self.storage
    if not params.when:
      return self.fn(*full_args, **kw)

    call_hash = self._get_call_hash(args, kw)
    call_id = f"{storage.fn_id()}/{call_hash}"
    refresh = rerun or not storage.exists(call_hash) or storage.expired(call_hash)

    if refresh:
      print_checkpoint(params.verbosity >= 1, "MEMORIZING", call_id, "blue")
      data = self.fn(*full_args, **kw)
      if iscoroutine(data):
        return self._store_coroutine(call_hash, data)
      return storage.store(call_hash, data)

    try:
      data = storage.load(call_hash)
      print_checkpoint(params.verbosity >= 2, "REMEMBERED", call_id, "green")
      if isinstance(data, AwaitableValue):
        return to_coroutine(data.value)  # type: ignore
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
    return self.storage.exists(self._get_call_hash(args, kw))

  def delete(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs):
    self.storage.delete(self._get_call_hash(args, kw))

  @overload
  def get(self: Callable[P, Coro[R]], *args: P.args, **kw: P.kwargs) -> R: ...
  @overload
  def get(self: Callable[P, R], *args: P.args, **kw: P.kwargs) -> R: ...
  def get(self, *args, **kw):
    call_hash = self._get_call_hash(args, kw)
    try:
      data = self.storage.load(call_hash)
      return data.value if isinstance(data, AwaitableValue) else data
    except Exception as ex:
      raise CheckpointError("Could not load checkpoint") from ex

  @overload
  def get_or(self: Callable[P, Coro[R]], default: T, *args: P.args, **kw: P.kwargs) -> R | T: ...
  @overload
  def get_or(self: Callable[P, R], default: T, *args: P.args, **kw: P.kwargs) -> R | T: ...
  def get_or(self, default, *args, **kw):
    try:
      return self.get(*args, **kw)  # type: ignore
    except CheckpointError:
      return default

  @overload
  def set(self: Callable[P, Coro[R]], value: AwaitableValue[R], *args: P.args, **kw: P.kwargs): ...
  @overload
  def set(self: Callable[P, R], value: R, *args: P.args, **kw: P.kwargs): ...
  def set(self, value, *args, **kw):
    self.storage.store(self._get_call_hash(args, kw), value)

  def set_awaitable(self: CachedFunction[Callable[P, Coro[R]]], value: R, *args: P.args, **kw: P.kwargs):
    self.set(AwaitableValue(value), *args, **kw)

  def __repr__(self) -> str:
    initialized = "fn_hash" in self.ident.__dict__
    fn_hash = self.ident.fn_hash[:6] if initialized else "- uninitialized"
    return f"<CachedFunction {self.fn.__name__} {fn_hash}>"
