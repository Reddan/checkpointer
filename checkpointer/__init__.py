from .checkpoint import create_checkpointer, read_only
from .storage import store_on_demand, read_from_store
from .function_body import get_function_hash, get_function_dir_path, get_function_project_dir_path

checkpoint = create_checkpointer()
