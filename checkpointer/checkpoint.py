from __future__ import annotations
import re
from datetime import datetime
from functools import cached_property, update_wrapper
from inspect import Parameter, iscoroutine, signature, unwrap
from itertools import chain
from pathlib import Path
from typing import (
  Callable, Concatenate, Coroutine, Generic, Iterable,
  Literal, Self, Type, TypedDict, Unpack, cast, overload,
)
from .fn_ident import Capturable, RawFunctionIdent, get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage, StorageType
from .types import AwaitableValue, C, Coro, Fn, P, R, hash_by_from_annotation

DEFAULT_DIR = Path.home() / ".cache/checkpoints"

class CheckpointError(Exception):
  pass

class CheckpointerOpts(TypedDict, total=False):
  format: Type[Storage] | StorageType
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

  def reset(self):
    self.__init__(self.cached_fn)

  def is_static(self) -> bool:
    return self.cached_fn.checkpointer.fn_hash_from is not None

  @cached_property
  def raw_ident(self) -> RawFunctionIdent:
    return get_fn_ident(unwrap(self.cached_fn.fn), self.cached_fn.checkpointer.capture)

  @cached_property
  def fn_hash(self) -> str:
    if self.is_static():
      return str(ObjectHash(self.cached_fn.checkpointer.fn_hash_from, digest_size=16))
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
    wrapped = unwrap(fn)
    fn_file = Path(wrapped.__code__.co_filename).name
    fn_name = re.sub(r"[^\w.]", "", wrapped.__qualname__)
    store_format = checkpointer.format
    Storage = STORAGE_MAP[store_format] if isinstance(store_format, str) else store_format
    update_wrapper(cast(Callable, self), wrapped)
    self.checkpointer = checkpointer
    self.fn = fn
    self.fn_dir = f"{fn_file}/{fn_name}"
    self.storage = Storage(self)
    self.cleanup = self.storage.cleanup
    self.bound = ()

    params = list(signature(wrapped).parameters.values())
    pos_params = (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD)
    self.arg_names = [param.name for param in params if param.kind in pos_params]
    self.default_args = {param.name: param.default for param in params if param.default is not Parameter.empty}
    self.hash_by_map = get_hash_by_map(params)
    self.ident = FunctionIdent(self)

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
  def depends(self) -> list[Callable]:
    return self.ident.raw_ident.depends

  def reinit(self, recursive=False) -> CachedFunction[Fn]:
    depend_idents = list(self.ident.deep_idents()) if recursive else [self.ident]
    for ident in depend_idents: ident.reset()
    for ident in depend_idents: ident.fn_hash
    return self

  def _get_call_hash(self, args: tuple, kw: dict[str, object]) -> str:
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
        pos_args = map(pos_hash_by, pos_args)
    named_args_iter = chain.from_iterable(sorted(named_args.items()))
    captured = chain.from_iterable(capturable.capture() for capturable in self.ident.capturables)
    obj_hash = ObjectHash(digest_size=16) \
      .update(iter=named_args_iter, header="NAMED") \
      .update(iter=pos_args, header="POS") \
      .update(iter=captured, header="CAPTURED")
    return str(obj_hash)

  def get_call_hash(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> str:
    return self._get_call_hash(args, kw)

  async def _store_coroutine(self, call_hash: str, coroutine: Coroutine):
    return self.storage.store(call_hash, AwaitableValue(await coroutine)).value

  def _call(self: CachedFunction[Callable[P, R]], args: tuple, kw: dict, rerun=False) -> R:
    full_args = self.bound + args
    params = self.checkpointer
    if not params.when:
      return self.fn(*full_args, **kw)

    call_hash = self._get_call_hash(args, kw)
    call_id = f"{self.storage.fn_id()}/{call_hash}"
    refresh = rerun \
      or not self.storage.exists(call_hash) \
      or (params.should_expire and params.should_expire(self.storage.checkpoint_date(call_hash)))

    if refresh:
      print_checkpoint(params.verbosity >= 1, "MEMORIZING", call_id, "blue")
      data = self.fn(*full_args, **kw)
      if iscoroutine(data):
        return self._store_coroutine(call_hash, data)
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
  def set(self: Callable[P, Coro[R]], value: AwaitableValue[R], *args: P.args, **kw: P.kwargs): ...
  @overload
  def set(self: Callable[P, R], value: R, *args: P.args, **kw: P.kwargs): ...
  def set(self, value, *args, **kw):
    self.storage.store(self._get_call_hash(args, kw), value)

  def __repr__(self) -> str:
    initialized = "fn_hash" in self.ident.__dict__
    fn_hash = self.ident.fn_hash[:6] if initialized else "- uninitialized"
    return f"<CachedFunction {self.fn.__name__} {fn_hash}>"

def get_hash_by_map(params: list[Parameter]) -> dict[str | bytes, Callable[[object], object]]:
  hash_by_map = {}
  for param in params:
    name = param.name
    if param.kind == Parameter.VAR_POSITIONAL:
      name = b"*"
    elif param.kind == Parameter.VAR_KEYWORD:
      name = b"**"
    hash_by_map[name] = hash_by_from_annotation(param.annotation)
  return hash_by_map if any(hash_by_map.values()) else {}
