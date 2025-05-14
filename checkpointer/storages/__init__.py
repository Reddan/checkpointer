from typing import Type
from .storage import Storage
from .pickle_storage import PickleStorage
from .memory_storage import MemoryStorage

STORAGE_MAP: dict[str, Type[Storage]] = {
  "pickle": PickleStorage,
  "memory": MemoryStorage,
}
