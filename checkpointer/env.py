import os
from pathlib import Path

default_dir = str(Path.home()) + '/.checkpoints'
storage_dir = os.environ.get('CHECKPOINTS_DIR', default_dir) + '/'
verbosity = os.environ.get('CHECKPOINTER_VERBOSITY', 'NORMAL')

mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost')
mongo_db = os.environ.get('MONGO_CHECKPOINTS_DB', 'checkpoints')
