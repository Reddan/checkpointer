from .checkpoint import Checkpointer, CheckpointFn
from .checkpoint import CheckpointError, CheckpointReadFail
from .types import Storage
from .function_body import get_function_hash

create_checkpointer = Checkpointer
checkpoint = Checkpointer()
memory_checkpoint = Checkpointer(format="memory")
tmp_checkpoint = Checkpointer(root_path="/tmp/checkpoints")
