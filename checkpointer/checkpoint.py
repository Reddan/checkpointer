from __future__ import annotations
import re
from datetime import datetime
from functools import cached_property, update_wrapper
from inspect import iscoroutine
from pathlib import Path
from typing import (
  Callable, Concatenate, Coroutine, Generic, Iterable, Literal,
  ParamSpec, Self, Type, TypedDict, TypeVar, Unpack, cast, overload
)
from .fn_ident import get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage
from .utils import AwaitableValue, unwrap_fn

Fn = TypeVar("Fn", bound=Callable)
P = ParamSpec("P")
R = TypeVar("R")
C = TypeVar("C")

DEFAULT_DIR = Path.home() / ".cache/checkpoints"

class CheckpointError(Exception):
  pass

class CheckpointerOpts(TypedDict, total=False):
  format: Type[Storage] | Literal["pickle", "memory", "bcolz"]
  root_path: Path | str | None
  when: bool
  verbosity: Literal[0, 1, 2]
  hash_by: Callable | None
  should_expire: Callable[[datetime], bool] | None
  capture: bool
  fn_hash: ObjectHash | None

class Checkpointer:
  def __init__(self, **opts: Unpack[CheckpointerOpts]):
    self.format = opts.get("format", "pickle")
    self.root_path = Path(opts.get("root_path", DEFAULT_DIR) or ".")
    self.when = opts.get("when", True)
    self.verbosity = opts.get("verbosity", 1)
    self.hash_by = opts.get("hash_by")
    self.should_expire = opts.get("should_expire")
    self.capture = opts.get("capture", False)
    self.fn_hash = opts.get("fn_hash")

  @overload
  def __call__(self, fn: Fn, **override_opts: Unpack[CheckpointerOpts]) -> CachedFunction[Fn]: ...
  @overload
  def __call__(self, fn: None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer: ...
  def __call__(self, fn: Fn | None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer | CachedFunction[Fn]:
    if override_opts:
      opts = CheckpointerOpts(**{**self.__dict__, **override_opts})
      return Checkpointer(**opts)(fn)

    return CachedFunction(self, fn) if callable(fn) else self

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

  @overload
  def __get__(self: Self, instance: None, owner: Type[C]) -> Self: ...
  @overload
  def __get__(self: CachedFunction[Callable[Concatenate[C, P], R]], instance: C, owner: Type[C]) -> CachedFunction[Callable[P, R]]: ...
  def __get__(self, instance, owner):
    if instance is None:
      return self
    bound_fn = object.__new__(CachedFunction)
    bound_fn.__dict__ |= self.__dict__
    bound_fn.bound = (instance,)
    return bound_fn

  @cached_property
  def ident_tuple(self) -> tuple[str, list[Callable]]:
    return get_fn_ident(unwrap_fn(self.fn), self.checkpointer.capture)

  @property
  def fn_hash_raw(self) -> str:
    return self.ident_tuple[0]

  @property
  def depends(self) -> list[Callable]:
    return self.ident_tuple[1]

  @cached_property
  def fn_hash(self) -> str:
    deep_hashes = [depend.fn_hash_raw for depend in self.deep_depends()]
    fn_hash = ObjectHash(digest_size=16).write_text(self.fn_hash_raw, *deep_hashes)
    return str(self.checkpointer.fn_hash or fn_hash)[:32]

  def reinit(self, recursive=False) -> CachedFunction[Fn]:
    depends = list(self.deep_depends()) if recursive else [self]
    for depend in depends:
      depend.__dict__.pop("fn_hash", None)
      depend.__dict__.pop("ident_tuple", None)
    for depend in depends:
      depend.fn_hash
    return self

  def get_call_id(self, args: tuple, kw: dict) -> str:
    args = self.bound + args
    hash_by = self.checkpointer.hash_by
    hash_params = hash_by(*args, **kw) if hash_by else (args, kw)
    return str(ObjectHash(hash_params, digest_size=16))

  async def _resolve_coroutine(self, call_id: str, coroutine: Coroutine):
    return self.storage.store(call_id, AwaitableValue(await coroutine)).value

  def _call(self: CachedFunction[Callable[P, R]], args: tuple, kw: dict, rerun=False) -> R:
    full_args = self.bound + args
    params = self.checkpointer
    if not params.when:
      return self.fn(*full_args, **kw)

    call_id = self.get_call_id(args, kw)
    call_id_long = f"{self.fn_dir}/{self.fn_hash}/{call_id}"

    refresh = rerun \
      or not self.storage.exists(call_id) \
      or (params.should_expire and params.should_expire(self.storage.checkpoint_date(call_id)))

    if refresh:
      print_checkpoint(params.verbosity >= 1, "MEMORIZING", call_id_long, "blue")
      data = self.fn(*full_args, **kw)
      if iscoroutine(data):
        return self._resolve_coroutine(call_id, data)
      return self.storage.store(call_id, data)

    try:
      data = self.storage.load(call_id)
      print_checkpoint(params.verbosity >= 2, "REMEMBERED", call_id_long, "green")
      return data
    except (EOFError, FileNotFoundError):
      pass
    print_checkpoint(params.verbosity >= 1, "CORRUPTED", call_id_long, "yellow")
    return self._call(args, kw, True)

  def __call__(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> R:
    return self._call(args, kw)

  def rerun(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> R:
    return self._call(args, kw, True)

  @overload
  def get(self: Callable[P, Coroutine[object, object, R]], *args: P.args, **kw: P.kwargs) -> R: ...
  @overload
  def get(self: Callable[P, R], *args: P.args, **kw: P.kwargs) -> R: ...
  def get(self, *args, **kw):
    call_id = self.get_call_id(args, kw)
    try:
      data = self.storage.load(call_id)
      return data.value if isinstance(data, AwaitableValue) else data
    except Exception as ex:
      raise CheckpointError("Could not load checkpoint") from ex

  def exists(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs) -> bool:
    return self.storage.exists(self.get_call_id(args, kw))

  def delete(self: CachedFunction[Callable[P, R]], *args: P.args, **kw: P.kwargs):
    self.storage.delete(self.get_call_id(args, kw))

  def __repr__(self) -> str:
    return f"<CachedFunction {self.fn.__name__} {self.fn_hash[:6]}>"

  def deep_depends(self, visited: set[CachedFunction] = set()) -> Iterable[CachedFunction]:
    if self not in visited:
      yield self
      visited = visited or set()
      visited.add(self)
      for depend in self.depends:
        if isinstance(depend, CachedFunction):
          yield from depend.deep_depends(visited)
