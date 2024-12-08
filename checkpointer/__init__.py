from .checkpoint import Checkpointer, CheckpointFn, CheckpointError
from .types import Storage
from .fn_ident import get_function_hash
import gc
import tempfile

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(format="memory", verbosity=0)
tmp_checkpoint = Checkpointer(root_path=tempfile.gettempdir() + "/checkpoints")

def cleanup_all(invalidated=True, expired=True):
  for obj in gc.get_objects():
    if isinstance(obj, CheckpointFn):
      obj.cleanup(invalidated=invalidated, expired=expired)
