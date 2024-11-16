import inspect
import relib.hashing as hashing
from typing import Generic, TypeVar, TypedDict, Unpack, Literal, Union, Any, cast, overload
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from functools import update_wrapper
from .types import Storage
from .function_body import get_function_hash
from .utils import unwrap_fn, sync_resolve_coroutine
from .storages.pickle_storage import PickleStorage
from .storages.memory_storage import MemoryStorage
from .storages.bcolz_storage import BcolzStorage
from .print_checkpoint import print_checkpoint

Fn = TypeVar("Fn", bound=Callable)

DEFAULT_DIR = Path.home() / ".cache/checkpoints"
STORAGE_MAP = {"memory": MemoryStorage, "pickle": PickleStorage, "bcolz": BcolzStorage}

class CheckpointError(Exception):
  pass

class CheckpointReadFail(CheckpointError):
  pass

StorageType = Literal["pickle", "memory", "bcolz"] | Storage
CheckpointPath = str | Callable[..., str] | None
ShouldExpire = Callable[[datetime], bool]

class CheckpointerOpts(TypedDict, total=False):
  format: StorageType
  root_path: Path | str | None
  when: bool
  verbosity: Literal[0, 1]
  path: CheckpointPath
  should_expire: ShouldExpire

class Checkpointer:
  def __init__(self, **opts: Unpack[CheckpointerOpts]):
    self.format = opts.get("format", "pickle")
    self.root_path = Path(opts.get("root_path", DEFAULT_DIR) or ".")
    self.when = opts.get("when", True)
    self.verbosity = opts.get("verbosity", 1)
    self.path = opts.get("path")
    self.should_expire = opts.get("should_expire")

  def get_storage(self) -> Storage:
    return STORAGE_MAP[self.format] if isinstance(self.format, str) else self.format

  @overload
  def __call__(self, fn: Fn, **override_opts: Unpack[CheckpointerOpts]) -> "CheckpointFn[Fn]": ...
  @overload
  def __call__(self, fn=None, **override_opts: Unpack[CheckpointerOpts]) -> "Checkpointer": ...
  def __call__(self, fn: Fn | None=None, **override_opts: Unpack[CheckpointerOpts]) -> Union["Checkpointer", "CheckpointFn[Fn]"]:
    if override_opts:
      opts = CheckpointerOpts(**{**self.__dict__, **override_opts})
      return Checkpointer(**opts)(fn)

    return CheckpointFn(self, fn) if callable(fn) else self

class CheckpointFn(Generic[Fn]):
  def __init__(self, checkpointer: Checkpointer, fn: Fn):
    wrapped = unwrap_fn(fn)
    file_name = Path(wrapped.__code__.co_filename).name
    update_wrapper(cast(Callable, self), wrapped)
    self.checkpointer = checkpointer
    self.fn = fn
    self.fn_hash = get_function_hash(wrapped)
    self.fn_id = f"{file_name}/{wrapped.__name__}"
    self.is_async = inspect.iscoroutinefunction(fn)

  def get_checkpoint_id(self, args: tuple, kw: dict) -> str:
    match self.checkpointer.path:
      case str() as path:
        return path
      case Callable() as path:
        p = path(*args, **kw)
        assert isinstance(p, str), "path function must return a string"
        return p
      case _:
        return f"{self.fn_id}/{hashing.hash([self.fn_hash, args, kw or 0])}"

  async def _store_on_demand(self, args: tuple, kw: dict, force: bool):
    checkpoint_id = self.get_checkpoint_id(args, kw)
    checkpoint_path = self.checkpointer.root_path / checkpoint_id
    storage = self.checkpointer.get_storage()
    should_log = storage is not MemoryStorage and self.checkpointer.verbosity > 0
    refresh = force \
      or storage.is_expired(checkpoint_path) \
      or (self.checkpointer.should_expire and storage.should_expire(checkpoint_path, self.checkpointer.should_expire))

    if refresh:
      print_checkpoint(should_log, "MEMORIZING", checkpoint_id, "blue")
      data = self.fn(*args, **kw)
      if inspect.iscoroutine(data):
        data = await data
      return storage.store_data(checkpoint_path, data)

    try:
      data = storage.load_data(checkpoint_path)
      print_checkpoint(should_log, "REMEMBERED", checkpoint_id, "green")
      return data
    except (EOFError, FileNotFoundError):
      print_checkpoint(should_log, "CORRUPTED", checkpoint_id, "yellow")
      storage.delete_data(checkpoint_path)
      return await self._store_on_demand(args, kw, force)

  def _call(self, args: tuple, kw: dict, force=False):
    if not self.checkpointer.when:
      return self.fn(*args, **kw)
    coroutine = self._store_on_demand(args, kw, force)
    return coroutine if self.is_async else sync_resolve_coroutine(coroutine)

  __call__: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw))
  rerun: Fn = cast(Fn, lambda self, *args, **kw: self._call(args, kw, True))

  def get(self, *args, **kw) -> Any:
    checkpoint_path = self.checkpointer.root_path / self.get_checkpoint_id(args, kw)
    try:
      return self.checkpointer.get_storage().load_data(checkpoint_path)
    except:
      raise CheckpointReadFail()
