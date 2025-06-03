import sys
import urllib.parse
from rich import print
from checkpointer import Checkpointer
from checkpointer.import_mappings import get_import_mappings
from .. import get_function_hash, storages, utils
from ..object_hash import ObjectHash
from ..storages import pickle_storage
from ..storages.memory_storage import MemoryStorage
from ..utils import AttrDict
from . import Storage, memory_storage
from .pickle_storage import PickleStorage

def test_import_mappings():
  self_module = sys.modules[__name__]
  import_mappings = get_import_mappings(self_module)
  assert import_mappings == {
    "sys": ("sys", None),
    "urllib.parse": ("urllib.parse", None),
    "print": ("rich", "print"),
    "Checkpointer": ("checkpointer", "Checkpointer"),
    "get_import_mappings": ("checkpointer.import_mappings", "get_import_mappings"),
    "get_function_hash": ("checkpointer", "get_function_hash"),
    "storages": ("checkpointer", "storages"),
    "utils": ("checkpointer", "utils"),
    "ObjectHash": ("checkpointer.object_hash", "ObjectHash"),
    "pickle_storage": ("checkpointer.storages", "pickle_storage"),
    "MemoryStorage": ("checkpointer.storages.memory_storage", "MemoryStorage"),
    "AttrDict": ("checkpointer.utils", "AttrDict"),
    "Storage": ("checkpointer.storages", "Storage"),
    "memory_storage": ("checkpointer.storages", "memory_storage"),
    "PickleStorage": ("checkpointer.storages.pickle_storage", "PickleStorage")
  }
  for name, (mod_name, attr_name) in import_mappings.items():
    origin = AttrDict.get_at(self_module, *mod_name.split("."))
    target_module = sys.modules[mod_name]
    if not attr_name:
      assert origin is target_module
    else:
      dest = getattr(target_module, attr_name)
      origin = getattr(self_module, name)
      assert origin is dest
