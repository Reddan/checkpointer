from typing import Literal, Type
from .memory_storage import MemoryStorage
from .pickle_storage import PickleStorage
from .storage import Storage

StorageType = Literal["pickle", "memory"]

STORAGE_MAP: dict[StorageType, Type[Storage]] = {
  "pickle": PickleStorage,
  "memory": MemoryStorage,
}
