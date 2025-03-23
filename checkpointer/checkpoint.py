from __future__ import annotations
import inspect
import re
from datetime import datetime
from functools import update_wrapper
from pathlib import Path
from typing import Any, Callable, Generic, Iterable, Literal, Type, TypedDict, TypeVar, Unpack, cast, overload
from .fn_ident import get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage
from .utils import resolved_awaitable, sync_resolve_coroutine, unwrap_fn

Fn = TypeVar("Fn", bound=Callable)

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
  fn_hash: str | None

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
  def __call__(self, fn: Fn, **override_opts: Unpack[CheckpointerOpts]) -> CheckpointFn[Fn]: ...
  @overload
  def __call__(self, fn: None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer: ...
  def __call__(self, fn: Fn | None=None, **override_opts: Unpack[CheckpointerOpts]) -> Checkpointer | CheckpointFn[Fn]:
    if override_opts:
      opts = CheckpointerOpts(**{**self.__dict__, **override_opts})
      return Checkpointer(**opts)(fn)

    return CheckpointFn(self, fn) if callable(fn) else self

class CheckpointFn(Generic[Fn]):
  def __init__(self, checkpointer: Checkpointer, fn: Fn):
    self.checkpointer = checkpointer
    self.fn = fn

  def _set_ident(self, force=False):
    if not hasattr(self, "fn_hash_raw") or force:
      self.fn_hash_raw, self.depends = get_fn_ident(unwrap_fn(self.fn), self.checkpointer.capture)
    return self

  def _lazyinit(self):
    wrapped = unwrap_fn(self.fn)
    fn_file = Path(wrapped.__code__.co_filename).name
    fn_name = re.sub(r"[^\w.]", "", wrapped.__qualname__)
    update_wrapper(cast(Callable, self), wrapped)
    store_format = self.checkpointer.format
    Storage = STORAGE_MAP[store_format] if isinstance(store_format, str) else store_format
    deep_hashes = [child._set_ident().fn_hash_raw for child in iterate_checkpoint_fns(self)]
    self.fn_hash = self.checkpointer.fn_hash or str(ObjectHash().write_text(self.fn_hash_raw, *deep_hashes))
    self.fn_subdir = f"{fn_file}/{fn_name}/{self.fn_hash[:16]}"
    self.is_async: bool = self.fn.is_async if isinstance(self.fn, CheckpointFn) else inspect.iscoroutinefunction(self.fn)
    self.storage = Storage(self)
    self.cleanup = self.storage.cleanup

  def __getattribute__(self, name: str) -> Any:
    return object.__getattribute__(self, "_getattribute")(name)

  def _getattribute(self, name: str) -> Any:
    setattr(self, "_getattribute", super().__getattribute__)
    self._lazyinit()
    return self._getattribute(name)

  def reinit(self, recursive=False) -> CheckpointFn[Fn]:
    pointfns = list(iterate_checkpoint_fns(self)) if recursive else [self]
    for pointfn in pointfns:
      pointfn._set_ident(True)
    for pointfn in pointfns:
      pointfn._lazyinit()
    return self

  def get_checkpoint_id(self, args: tuple, kw: dict) -> str:
    hash_params = [self.checkpointer.hash_by(*args, **kw)] if self.checkpointer.hash_by else (args, kw)
    call_hash = ObjectHash(self.fn_hash, *hash_params, digest_size=16)
    return f"{self.fn_subdir}/{call_hash}"

  async def _store_on_demand(self, args: tuple, kw: dict, rerun: bool):
    checkpoint_id = self.get_checkpoint_id(args, kw)
    checkpoint_path = self.checkpointer.root_path / checkpoint_id
    verbosity = self.checkpointer.verbosity
    refresh = rerun \
      or not self.storage.exists(checkpoint_path) \
      or (self.checkpointer.should_expire and self.checkpointer.should_expire(self.storage.checkpoint_date(checkpoint_path)))

    if refresh:
      print_checkpoint(verbosity >= 1, "MEMORIZING", checkpoint_id, "blue")
      data = self.fn(*args, **kw)
      if inspect.iscoroutine(data):
        data = await data
      self.storage.store(checkpoint_path, data)
      return data

    try:
      data = self.storage.load(checkpoint_path)
      print_checkpoint(verbosity >= 2, "REMEMBERED", checkpoint_id, "green")
      return data
    except (EOFError, FileNotFoundError):
      pass
    print_checkpoint(verbosity >= 1, "CORRUPTED", checkpoint_id, "yellow")
    return await self._store_on_demand(args, kw, True)

  def _call(self, args: tuple, kw: dict, rerun=False):
    if not self.checkpointer.when:
      return self.fn(*args, **kw)
    coroutine = self._store_on_demand(args, kw, rerun)
    return coroutine if self.is_async else sync_resolve_coroutine(coroutine)

  def _get(self, args, kw) -> Any:
    checkpoint_path = self.checkpointer.root_path / self.get_checkpoint_id(args, kw)
    try:
      val = self.storage.load(checkpoint_path)
      return resolved_awaitable(val) if self.is_async else val
    except Exception as ex:
      raise CheckpointError("Could not load checkpoint") from ex

  def exists(self, *args: tuple, **kw: dict) -> bool:
    return self.storage.exists(self.checkpointer.root_path / self.get_checkpoint_id(args, kw))

  __call__: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw))
  rerun: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw, True))
  get: Fn = cast(Fn, lambda self, *args, **kw: self._get(args, kw))

  def __repr__(self) -> str:
    return f"<CheckpointFn {self.fn.__name__} {self.fn_hash[:6]}>"

def iterate_checkpoint_fns(pointfn: CheckpointFn, visited: set[CheckpointFn] = set()) -> Iterable[CheckpointFn]:
  visited = visited or set()
  if pointfn not in visited:
    yield pointfn
    visited.add(pointfn)
    for depend in pointfn.depends:
      if isinstance(depend, CheckpointFn):
        yield from iterate_checkpoint_fns(depend, visited)
