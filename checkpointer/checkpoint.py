from __future__ import annotations
import inspect
import relib.hashing as hashing
from typing import Generic, TypeVar, Type, TypedDict, Callable, Unpack, Literal, Any, cast, overload
from pathlib import Path
from datetime import datetime
from functools import update_wrapper
from .types import Storage
from .function_body import get_function_hash
from .utils import unwrap_fn, sync_resolve_coroutine, resolved_awaitable
from .storages import STORAGE_MAP
from .print_checkpoint import print_checkpoint

Fn = TypeVar("Fn", bound=Callable)

DEFAULT_DIR = Path.home() / ".cache/checkpoints"

class CheckpointError(Exception):
  pass

class CheckpointerOpts(TypedDict, total=False):
  format: Type[Storage] | Literal["pickle", "memory", "bcolz"]
  root_path: Path | str | None
  when: bool
  verbosity: Literal[0, 1]
  path: Callable[..., str] | None
  should_expire: Callable[[datetime], bool] | None
  capture: bool

class Checkpointer:
  def __init__(self, **opts: Unpack[CheckpointerOpts]):
    self.format = opts.get("format", "pickle")
    self.root_path = Path(opts.get("root_path", DEFAULT_DIR) or ".")
    self.when = opts.get("when", True)
    self.verbosity = opts.get("verbosity", 1)
    self.path = opts.get("path")
    self.should_expire = opts.get("should_expire")
    self.capture = opts.get("capture", False)

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
    wrapped = unwrap_fn(fn)
    file_name = Path(wrapped.__code__.co_filename).name
    update_wrapper(cast(Callable, self), wrapped)
    storage = STORAGE_MAP[checkpointer.format] if isinstance(checkpointer.format, str) else checkpointer.format
    self.checkpointer = checkpointer
    self.fn = fn
    self.fn_hash, self.depends = get_function_hash(wrapped, self.checkpointer.capture)
    self.fn_id = f"{file_name}/{wrapped.__name__}"
    self.is_async = inspect.iscoroutinefunction(wrapped)
    self.storage = storage(checkpointer)

  def get_checkpoint_id(self, args: tuple, kw: dict) -> str:
    if not callable(self.checkpointer.path):
      # TODO: use digest size before digesting instead of truncating the hash
      call_hash = hashing.hash((self.fn_hash, args, kw), "blake2b")[:32]
      return f"{self.fn_id}/{call_hash}"
    checkpoint_id = self.checkpointer.path(*args, **kw)
    if not isinstance(checkpoint_id, str):
      raise CheckpointError(f"path function must return a string, got {type(checkpoint_id)}")
    return checkpoint_id

  async def _store_on_demand(self, args: tuple, kw: dict, rerun: bool):
    checkpoint_id = self.get_checkpoint_id(args, kw)
    checkpoint_path = self.checkpointer.root_path / checkpoint_id
    should_log = self.checkpointer.verbosity > 0
    refresh = rerun \
      or not self.storage.exists(checkpoint_path) \
      or (self.checkpointer.should_expire and self.checkpointer.should_expire(self.storage.checkpoint_date(checkpoint_path)))

    if refresh:
      print_checkpoint(should_log, "MEMORIZING", checkpoint_id, "blue")
      data = self.fn(*args, **kw)
      if inspect.iscoroutine(data):
        data = await data
      self.storage.store(checkpoint_path, data)
      return data

    try:
      data = self.storage.load(checkpoint_path)
      print_checkpoint(should_log, "REMEMBERED", checkpoint_id, "green")
      return data
    except (EOFError, FileNotFoundError):
      print_checkpoint(should_log, "CORRUPTED", checkpoint_id, "yellow")
      self.storage.delete(checkpoint_path)
      return await self._store_on_demand(args, kw, rerun)

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
    except:
      raise CheckpointError("Could not load checkpoint")

  def exists(self, *args: tuple, **kw: dict) -> bool:
    return self.storage.exists(self.checkpointer.root_path / self.get_checkpoint_id(args, kw))

  __call__: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw))
  rerun: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw, True))
  get: Fn = cast(Fn, lambda self, *args, **kw: self._get(args, kw))
