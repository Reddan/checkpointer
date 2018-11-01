import os

from .checkpoint import create_checkpointer, read_only, default_dir
from .storage import store_on_demand, read_from_store
from .function_body import get_function_hash, get_function_dir_path, get_function_project_dir_path

storage_dir = os.environ.get('CHECKPOINTS_DIR', default_dir)
verbosity = int(os.environ.get('CHECKPOINTS_VERBOSITY', '1'))

checkpoint = create_checkpointer(dir=storage_dir, verbosity=verbosity)
