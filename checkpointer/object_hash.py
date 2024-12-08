import ctypes
import hashlib
import io
import re
from collections.abc import Iterable
from contextlib import nullcontext, suppress
from decimal import Decimal
from itertools import chain
from pickle import HIGHEST_PROTOCOL as PROTOCOL
from types import BuiltinFunctionType, FunctionType, GeneratorType, MethodType, ModuleType, UnionType
from typing import Any, TypeAliasType, TypeVar
from .utils import ContextVar, get_fn_body

np, torch = None, None

with suppress(Exception):
  import numpy as np

with suppress(Exception):
  import torch

def encode_type(t: type | FunctionType) -> str:
  return f"{t.__module__}:{t.__qualname__}"

def encode_val(v: Any) -> str:
  return encode_type(type(v))

class ObjectHashError(Exception):
  def __init__(self, obj: Any, cause: Exception):
    super().__init__(f"{type(cause).__name__} error when hashing {obj}")
    self.obj = obj

class ObjectHash:
  def __init__(self, *obj: Any, iter: Iterable[Any] = [], digest_size=64, tolerate_errors=False) -> None:
    self.hash = hashlib.blake2b(digest_size=digest_size)
    self.current: dict[int, int] = {}
    self.tolerate_errors = ContextVar(tolerate_errors)
    self.update(iter=chain(obj, iter))

  def copy(self) -> "ObjectHash":
    new = ObjectHash(tolerate_errors=self.tolerate_errors.value)
    new.hash = self.hash.copy()
    return new

  def hexdigest(self) -> str:
    return self.hash.hexdigest()

  __str__ = hexdigest

  def update_hash(self, *data: bytes | str, iter: Iterable[bytes | str] = []) -> "ObjectHash":
    for d in chain(data, iter):
      self.hash.update(d.encode() if isinstance(d, str) else d)
    return self

  def header(self, *args: Any) -> "ObjectHash":
    return self.update_hash(":".join(map(str, args)))

  def update(self, *objs: Any, iter: Iterable[Any] = [], tolerate_errors: bool | None=None) -> "ObjectHash":
    with nullcontext() if tolerate_errors is None else self.tolerate_errors.set(tolerate_errors):
      for obj in chain(objs, iter):
        try:
          self._update_one(obj)
        except Exception as ex:
          if self.tolerate_errors.value:
            self.header("error").update(type(ex))
            continue
          raise ObjectHashError(obj, ex) from ex
      return self

  def _update_one(self, obj: Any) -> None:
    match obj:
      case None:
        self.header("null")

      case bool() | int() | float() | complex() | Decimal() | ObjectHash():
        self.header("number", encode_val(obj), obj)

      case str() | bytes() | bytearray() | memoryview():
        self.header("bytes", encode_val(obj), len(obj)).update_hash(obj)

      case set() | frozenset():
        self.header("set", encode_val(obj), len(obj))
        try:
          items = sorted(obj)
        except:
          self.header("unsortable")
          items = sorted(str(ObjectHash(item, tolerate_errors=self.tolerate_errors.value)) for item in obj)
        self.update(iter=items)

      case TypeVar():
        self.header("TypeVar").update(obj.__name__, obj.__bound__, obj.__constraints__, obj.__contravariant__, obj.__covariant__)

      case TypeAliasType():
        self.header("TypeAliasType").update(obj.__name__, obj.__value__)

      case UnionType():
        self.header("UnionType").update(obj.__args__)

      case BuiltinFunctionType():
        self.header("builtin", obj.__qualname__)

      case FunctionType():
        self.header("function", encode_type(obj)).update(get_fn_body(obj), obj.__defaults__, obj.__kwdefaults__, obj.__annotations__)

      case MethodType():
        self.header("method").update(obj.__func__, obj.__self__.__class__)

      case ModuleType():
        self.header("module", obj.__name__, obj.__file__)

      case GeneratorType():
        self.header("generator", obj.__qualname__)._update_iterator(obj)

      case io.TextIOWrapper() | io.FileIO() | io.BufferedRandom() | io.BufferedWriter() | io.BufferedReader():
        self.header("file", encode_val(obj)).update(obj.name, obj.mode, obj.tell())

      case type():
        self.header("type", encode_type(obj))

      case _ if np and isinstance(obj, np.dtype):
        self.header("dtype").update(obj.__class__, obj.descr)

      case _ if np and isinstance(obj, np.ndarray):
        self.header("ndarray", encode_val(obj), obj.shape, obj.strides).update(obj.dtype)
        if obj.dtype.hasobject:
          self.update(obj.__reduce_ex__(PROTOCOL))
        else:
          array = np.ascontiguousarray(obj if obj.base is None else obj.base).view(np.uint8)
          self.update_hash(array.data)

      case _ if torch and isinstance(obj, torch.Tensor):
        self.header("tensor", encode_val(obj), obj.dtype, tuple(obj.shape), obj.stride(), obj.device)
        if obj.device.type != "cpu":
          obj = obj.cpu()
        storage = obj.storage()
        buffer = (ctypes.c_ubyte * storage.nbytes()).from_address(storage.data_ptr())
        self.update_hash(memoryview(buffer))

      case _ if id(obj) in self.current:
        self.header("circular", self.current[id(obj)])

      case _:
        try:
          self.current[id(obj)] = len(self.current)
          match obj:
            case list() | tuple():
              self.header("list", encode_val(obj), len(obj)).update(iter=obj)
            case dict():
              try:
                items = sorted(obj.items())
              except:
                items = sorted((str(ObjectHash(key, tolerate_errors=self.tolerate_errors.value)), val) for key, val in obj.items())
              self.header("dict", encode_val(obj), len(obj)).update(iter=chain.from_iterable(items))
            case _:
              self._update_object(obj)
        finally:
          del self.current[id(obj)]

  def _update_iterator(self, obj: Iterable) -> None:
    self.header("iterator", encode_val(obj)).update(iter=obj).header(b"iterator-end")

  def _update_object(self, obj: object) -> "ObjectHash":
    self.header("instance", encode_val(obj))
    try:
      reduced = obj.__reduce_ex__(PROTOCOL) if hasattr(obj, "__reduce_ex__") else obj.__reduce__()
    except:
      reduced = None
    if isinstance(reduced, str):
      return self.header("reduce-str").update(reduced)
    if reduced:
      reduced = list(reduced)
      it = reduced.pop(3) if len(reduced) >= 4 else None
      self.header("reduce").update(reduced)
      if it is not None:
        self._update_iterator(it)
      return self
    if state := hasattr(obj, "__getstate__") and obj.__getstate__():
      return self.header("getstate").update(state)
    if len(getattr(obj, "__slots__", [])):
      slots = {slot: getattr(obj, slot, None) for slot in getattr(obj, "__slots__")}
      return self.header("slots").update(slots)
    if d := getattr(obj, "__dict__", {}):
      return self.header("dict").update(d)
    repr_str = re.sub(r"\s+(at\s+0x[0-9a-fA-F]+)(>)$", r"\2", repr(obj))
    return self.header("repr").update(repr_str)
