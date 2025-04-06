import ctypes
import hashlib
import io
import re
import sys
from collections.abc import Iterable
from contextlib import nullcontext, suppress
from decimal import Decimal
from itertools import chain
from pickle import HIGHEST_PROTOCOL as PROTOCOL
from types import BuiltinFunctionType, FunctionType, GeneratorType, MethodType, ModuleType, UnionType
from typing import Any, TypeVar
from .utils import ContextVar, get_fn_body

np, torch = None, None

with suppress(Exception):
  import numpy as np
with suppress(Exception):
  import torch

class _Never:
  def __getattribute__(self, _: str):
    pass

if sys.version_info >= (3, 12):
  from typing import TypeAliasType
else:
  TypeAliasType = _Never

def encode_type(t: type | FunctionType) -> str:
  return f"{t.__module__}:{t.__qualname__}"

def encode_type_of(v: Any) -> str:
  return encode_type(type(v))

class ObjectHashError(Exception):
  def __init__(self, obj: Any, cause: Exception):
    super().__init__(f"{type(cause).__name__} error when hashing {obj}")
    self.obj = obj

class ObjectHash:
  def __init__(self, *objs: Any, iter: Iterable[Any] = (), digest_size=64, tolerate_errors=False) -> None:
    self.hash = hashlib.blake2b(digest_size=digest_size)
    self.current: dict[int, int] = {}
    self.tolerate_errors = ContextVar(tolerate_errors)
    self.update(iter=chain(objs, iter))

  def copy(self) -> "ObjectHash":
    new = ObjectHash(tolerate_errors=self.tolerate_errors.value)
    new.hash = self.hash.copy()
    return new

  def hexdigest(self) -> str:
    return self.hash.hexdigest()

  __str__ = hexdigest

  def __eq__(self, value: object) -> bool:
    return isinstance(value, ObjectHash) and str(self) == str(value)

  def nested_hash(self, *objs: Any) -> str:
    return ObjectHash(iter=objs, tolerate_errors=self.tolerate_errors.value).hexdigest()

  def write_bytes(self, *data: bytes, iter: Iterable[bytes] = ()) -> "ObjectHash":
    for d in chain(data, iter):
      self.hash.update(d)
    return self

  def write_text(self, *data: str, iter: Iterable[str] = ()) -> "ObjectHash":
    return self.write_bytes(iter=(d.encode() for d in chain(data, iter)))

  def header(self, *args: Any) -> "ObjectHash":
    return self.write_bytes(":".join(map(str, args)).encode())

  def update(self, *objs: Any, iter: Iterable[Any] = (), tolerate_errors: bool | None=None) -> "ObjectHash":
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
        self.header("number", encode_type_of(obj), obj)

      case str() | bytes() | bytearray() | memoryview():
        b = obj.encode() if isinstance(obj, str) else obj
        self.header("bytes", encode_type_of(obj), len(b)).write_bytes(b)

      case set() | frozenset():
        try:
          items = sorted(obj)
          header = "set"
        except:
          items = sorted(map(self.nested_hash, obj))
          header = "set-unsortable"
        self.header(header, encode_type_of(obj), len(obj)).update(iter=items)

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
        self.header("file", encode_type_of(obj)).update(obj.name, obj.mode, obj.tell())

      case type():
        self.header("type", encode_type(obj))

      case _ if np and isinstance(obj, np.dtype):
        self.header("dtype").update(obj.__class__, obj.descr)

      case _ if np and isinstance(obj, np.ndarray):
        self.header("ndarray", encode_type_of(obj), obj.shape, obj.strides).update(obj.dtype)
        if obj.dtype.hasobject:
          self.update(obj.__reduce_ex__(PROTOCOL))
        else:
          array = np.ascontiguousarray(obj if obj.base is None else obj.base).view(np.uint8)
          self.write_bytes(array.data)

      case _ if torch and isinstance(obj, torch.Tensor):
        self.header("tensor", encode_type_of(obj), obj.dtype, tuple(obj.shape), obj.stride(), obj.device)
        if obj.device.type != "cpu":
          obj = obj.cpu()
        storage = obj.storage()
        buffer = (ctypes.c_ubyte * storage.nbytes()).from_address(storage.data_ptr())
        self.write_bytes(memoryview(buffer))

      case _ if id(obj) in self.current:
        self.header("circular", self.current[id(obj)])

      case _:
        try:
          self.current[id(obj)] = len(self.current)
          match obj:
            case list() | tuple():
              self.header("list", encode_type_of(obj), len(obj)).update(iter=obj)
            case dict():
              try:
                items = sorted(obj.items())
                header = "dict"
              except:
                items = sorted((self.nested_hash(key), val) for key, val in obj.items())
                header = "dict-unsortable"
              self.header(header, encode_type_of(obj), len(obj)).update(iter=chain.from_iterable(items))
            case _:
              self._update_object(obj)
        finally:
          del self.current[id(obj)]

  def _update_iterator(self, obj: Iterable) -> "ObjectHash":
    return self.header("iterator", encode_type_of(obj)).update(iter=obj).header("iterator-end")

  def _update_object(self, obj: object) -> "ObjectHash":
    self.header("instance", encode_type_of(obj))
    reduced = None
    with suppress(Exception):
      reduced = obj.__reduce_ex__(PROTOCOL)
    with suppress(Exception):
      reduced = reduced or obj.__reduce__()
    if isinstance(reduced, str):
      return self.header("reduce-str").update(reduced)
    if reduced:
      reduced = list(reduced)
      it = reduced.pop(3) if len(reduced) >= 4 else None
      return self.header("reduce").update(reduced)._update_iterator(it or ())
    if state := hasattr(obj, "__getstate__") and obj.__getstate__():
      return self.header("getstate").update(state)
    if len(getattr(obj, "__slots__", [])):
      slots = {slot: getattr(obj, slot, None) for slot in getattr(obj, "__slots__")}
      return self.header("slots").update(slots)
    if d := getattr(obj, "__dict__", {}):
      return self.header("dict").update(d)
    repr_str = re.sub(r"\s+(at\s+0x[0-9a-fA-F]+)(>)$", r"\2", repr(obj))
    return self.header("repr").update(repr_str)
