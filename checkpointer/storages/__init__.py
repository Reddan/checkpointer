from typing import Type
from .storage import Storage
from .pickle_storage import PickleStorage
from .memory_storage import MemoryStorage
from .bcolz_storage import BcolzStorage

STORAGE_MAP: dict[str, Type[Storage]] = {
  "pickle": PickleStorage,
  "memory": MemoryStorage,
  "bcolz": BcolzStorage,
}
