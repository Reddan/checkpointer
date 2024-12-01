from .checkpoint import Checkpointer, CheckpointFn, CheckpointError
from .types import Storage
from .function_body import get_function_hash
import tempfile

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
capture_checkpoint = Checkpointer(capture=True)
memory_checkpoint = Checkpointer(format="memory", verbosity=0)
tmp_checkpoint = Checkpointer(root_path=tempfile.gettempdir() + "/checkpoints")
