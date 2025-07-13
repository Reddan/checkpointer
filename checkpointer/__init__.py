import gc
import tempfile
from datetime import timedelta
from typing import Callable
from .checkpoint import CachedFunction, Checkpointer, CheckpointError, FunctionIdent
from .object_hash import ObjectHash
from .storages import MemoryStorage, PickleStorage, Storage
from .storages.memory_storage import cleanup_memory_storage
from .types import AwaitableValue, Captured, CapturedOnce, CaptureMe, CaptureMeOnce, HashBy, NoHash

checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(storage="memory", verbosity=0)
tmp_checkpoint = Checkpointer(directory=f"{tempfile.gettempdir()}/checkpoints")
static_checkpoint = Checkpointer(fn_hash_from=())

def cleanup_all(invalidated=True, expired=True):
  for obj in gc.get_objects():
    if isinstance(obj, CachedFunction):
      obj.cleanup(invalidated=invalidated, expired=expired)

def get_function_hash(fn: Callable) -> str:
  return CachedFunction(Checkpointer(), fn).ident.fn_hash
