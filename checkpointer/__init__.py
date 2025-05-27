import gc
import tempfile
from typing import Callable
from .checkpoint import CachedFunction, Checkpointer, CheckpointError
from .object_hash import ObjectHash
from .storages import MemoryStorage, PickleStorage, Storage
from .types import AwaitableValue, HashBy, NoHash

checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(format="memory", verbosity=0)
tmp_checkpoint = Checkpointer(root_path=f"{tempfile.gettempdir()}/checkpoints")
static_checkpoint = Checkpointer(fn_hash_from=())

def cleanup_all(invalidated=True, expired=True):
  for obj in gc.get_objects():
    if isinstance(obj, CachedFunction):
      obj.cleanup(invalidated=invalidated, expired=expired)

def get_function_hash(fn: Callable) -> str:
  return CachedFunction(Checkpointer(), fn).ident.fn_hash
