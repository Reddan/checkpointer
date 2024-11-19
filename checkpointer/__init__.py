from .checkpoint import Checkpointer, CheckpointFn, CheckpointError
from .types import Storage
from .function_body import get_function_hash
import tempfile

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
memory_checkpoint = Checkpointer(format="memory")
tmp_checkpoint = Checkpointer(root_path=tempfile.gettempdir() + "/checkpoints")
