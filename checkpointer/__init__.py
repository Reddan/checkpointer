import gc
import tempfile
from typing import Callable
from .checkpoint import Checkpointer, CheckpointError, CheckpointFn
from .object_hash import ObjectHash
from .storages import MemoryStorage, PickleStorage, Storage

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(format="memory", verbosity=0)
tmp_checkpoint = Checkpointer(root_path=tempfile.gettempdir() + "/checkpoints")

def cleanup_all(invalidated=True, expired=True):
  for obj in gc.get_objects():
    if isinstance(obj, CheckpointFn):
      obj.cleanup(invalidated=invalidated, expired=expired)

def get_function_hash(fn: Callable, capture=False) -> str:
  return CheckpointFn(Checkpointer(capture=capture), fn).fn_hash
