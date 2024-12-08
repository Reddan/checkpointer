import tempfile
from .checkpoint import Checkpointer, CheckpointError, CheckpointFn
from .fn_ident import get_function_hash
from .object_hash import ObjectHash
from .storages import MemoryStorage, PickleStorage
from .types import Storage

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(format="memory", verbosity=0)
tmp_checkpoint = Checkpointer(root_path=tempfile.gettempdir() + "/checkpoints")
