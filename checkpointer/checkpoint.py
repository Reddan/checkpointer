from __future__ import annotations
import inspect
import re
from datetime import datetime
from functools import update_wrapper
from pathlib import Path
from typing import Any, Awaitable, Callable, Generic, Iterable, Literal, ParamSpec, Type, TypedDict, TypeVar, Unpack, cast, overload
from .fn_ident import get_fn_ident
from .object_hash import ObjectHash
from .print_checkpoint import print_checkpoint
from .storages import STORAGE_MAP, Storage
from .utils import AwaitableValue, unwrap_fn

Fn = TypeVar("Fn", bound=Callable)
P = ParamSpec("P")
R = TypeVar("R")

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
    params = self.checkpointer
    wrapped = unwrap_fn(self.fn)
    fn_file = Path(wrapped.__code__.co_filename).name
    fn_name = re.sub(r"[^\w.]", "", wrapped.__qualname__)
    update_wrapper(cast(Callable, self), wrapped)
    Storage = STORAGE_MAP[params.format] if isinstance(params.format, str) else params.format
    deep_hashes = [child._set_ident().fn_hash_raw for child in iterate_checkpoint_fns(self)]
    self.fn_hash = str(params.fn_hash or ObjectHash(digest_size=16).write_text(self.fn_hash_raw, *deep_hashes))
    self.fn_subdir = f"{fn_file}/{fn_name}/{self.fn_hash[:32]}"
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
    hash_by = self.checkpointer.hash_by
    hash_params = hash_by(*args, **kw) if hash_by else (args, kw)
    call_hash = ObjectHash(hash_params, digest_size=16)
    return f"{self.fn_subdir}/{call_hash}"

  async def _resolve_awaitable(self, checkpoint_path: Path, awaitable: Awaitable):
    data = await awaitable
    self.storage.store(checkpoint_path, AwaitableValue(data))
    return data

  def _call(self, args: tuple, kw: dict, rerun=False):
    params = self.checkpointer
    if not params.when:
      return self.fn(*args, **kw)

    checkpoint_id = self.get_checkpoint_id(args, kw)
    checkpoint_path = params.root_path / checkpoint_id

    refresh = rerun \
      or not self.storage.exists(checkpoint_path) \
      or (params.should_expire and params.should_expire(self.storage.checkpoint_date(checkpoint_path)))

    if refresh:
      print_checkpoint(params.verbosity >= 1, "MEMORIZING", checkpoint_id, "blue")
      data = self.fn(*args, **kw)
      if inspect.isawaitable(data):
        return self._resolve_awaitable(checkpoint_path, data)
      else:
        self.storage.store(checkpoint_path, data)
        return data

    try:
      data = self.storage.load(checkpoint_path)
      print_checkpoint(params.verbosity >= 2, "REMEMBERED", checkpoint_id, "green")
      return data
    except (EOFError, FileNotFoundError):
      pass
    print_checkpoint(params.verbosity >= 1, "CORRUPTED", checkpoint_id, "yellow")
    return self._call(args, kw, True)

  __call__: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw))
  rerun: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw, True))

  @overload
  def get(self: Callable[P, Awaitable[R]], *args: P.args, **kw: P.kwargs) -> R: ...
  @overload
  def get(self: Callable[P, R], *args: P.args, **kw: P.kwargs) -> R: ...

  def get(self, *args, **kw):
    checkpoint_path = self.checkpointer.root_path / self.get_checkpoint_id(args, kw)
    try:
      data = self.storage.load(checkpoint_path)
      return data.value if isinstance(data, AwaitableValue) else data
    except Exception as ex:
      raise CheckpointError("Could not load checkpoint") from ex

  def exists(self: Callable[P, R], *args: P.args, **kw: P.kwargs) -> bool: # type: ignore
    self = cast(CheckpointFn, self)
    return self.storage.exists(self.checkpointer.root_path / self.get_checkpoint_id(args, kw))

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
