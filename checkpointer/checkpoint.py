import inspect
import relib.hashing as hashing
from typing import TypedDict, Unpack, Literal, Any, cast
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from functools import wraps
from .types import Storage
from .function_body import get_function_hash
from .utils import unwrap_fn, sync_resolve_coroutine
from .storages.pickle_storage import PickleStorage
from .storages.memory_storage import MemoryStorage
from .storages.bcolz_storage import BcolzStorage
from .print_checkpoint import print_checkpoint

DEFAULT_DIR = Path.home() / ".cache/checkpoints"
STORAGE_MAP = {"memory": MemoryStorage, "pickle": PickleStorage, "bcolz": BcolzStorage}

def get_checkpoint_id(fn: Callable, fn_hash: str, path: "CheckpointPath", args: tuple, kw: dict) -> str:
  if isinstance(path, str):
    return path
  elif callable(path):
    x = path(*args, **kw)
    assert isinstance(x, str), "path function must return a string"
    return x
  else:
    params_hash = hashing.hash([fn_hash, args, kw or 0])
    file_name = Path(fn.__code__.co_filename).name
    return "/".join((file_name, fn.__name__, params_hash))

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

  async def _store_on_demand(self, checkpoint_id: str, compute: Callable[[], Any], force: bool):
    checkpoint_path = self.root_path / checkpoint_id
    storage = self.get_storage()
    should_log = storage != MemoryStorage and self.verbosity != 0
    refresh = force \
      or storage.is_expired(checkpoint_path) \
      or (self.should_expire and storage.should_expire(checkpoint_path, self.should_expire))

    if refresh:
      print_checkpoint(should_log, "MEMORIZING", checkpoint_id, "blue")
      data = compute()
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
      result = await self._store_on_demand(checkpoint_id, compute, force)
      return result

  def __call__(self, opt_fn: Callable | None=None, **override_opts: Unpack[CheckpointerOpts]):
    if override_opts:
      opts = CheckpointerOpts(**{**self.__dict__, **override_opts})
      return Checkpointer(**opts)(opt_fn)

    def receive_fn(fn: Callable):
      if not self.when:
        return fn

      is_async = inspect.iscoroutinefunction(fn)
      unwrapped_fn = unwrap_fn(fn)
      fn_hash = get_function_hash(unwrapped_fn)

      @wraps(unwrapped_fn)
      def wrapper(*args, **kw):
        compute = lambda: fn(*args, **kw)
        recheck = kw.pop("recheck", False)
        checkpoint_id = get_checkpoint_id(unwrapped_fn, fn_hash, self.path, args, kw)
        coroutine = self._store_on_demand(checkpoint_id, compute, recheck)
        return coroutine if is_async else sync_resolve_coroutine(coroutine)

      setattr(wrapper, "checkpointer", self)
      return wrapper

    return receive_fn(opt_fn) if callable(opt_fn) else receive_fn

  def get(self, fn: Callable, args=[], kw={}, path: CheckpointPath=None) -> Any:
    unwrapped_fn = unwrap_fn(fn)
    fn_hash = get_function_hash(unwrapped_fn)
    checkpoint_path = self.root_path / get_checkpoint_id(unwrapped_fn, fn_hash, path, args, kw)
    try:
      return self.get_storage().load_data(checkpoint_path)
    except:
      return None
