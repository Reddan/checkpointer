import os
from pathlib import Path

default_dir = str(Path.home()) + '/.checkpoints'
storage_dir = os.environ.get('CHECKPOINTS_DIR', default_dir) + '/'
verbosity = os.environ.get('CHECKPOINTER_VERBOSITY', 'NORMAL')

