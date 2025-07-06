import ctypes
import hashlib
import inspect
import io
import re
import sys
import tokenize
import sysconfig
from collections import OrderedDict
from collections.abc import Iterable
from contextlib import nullcontext, suppress
from decimal import Decimal
from io import StringIO
from inspect import getfile
from itertools import chain
from pathlib import Path
from pickle import HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from types import BuiltinFunctionType, FunctionType, GeneratorType, MappingProxyType, MethodType, ModuleType, UnionType
from typing import Callable, Self, TypeVar
from .utils import ContextVar, flatten

np, torch = None, None

class _Never:
  def __getattribute__(self, _: str):
    pass

with suppress(Exception):
  import numpy as np
with suppress(Exception):
  import torch
if sys.version_info >= (3, 12):
  from typing import TypeAliasType
else:
  TypeAliasType = _Never

nc = nullcontext()
stdlib = Path(sysconfig.get_paths()["stdlib"]).resolve()

def encode_type(t: type | FunctionType) -> str:
  return f"{t.__module__}:{t.__qualname__}"

def encode_type_of(v: object) -> str:
  return encode_type(type(v))

class ObjectHashError(Exception):
  def __init__(self, obj: object, cause: Exception):
    super().__init__(f"{type(cause).__name__} error when hashing {obj}")
    self.obj = obj

class ObjectHash:
  def __init__(self, *objs: object, iter: Iterable[object] = (), digest_size=64, tolerable=False) -> None:
    self.hash = hashlib.blake2b(digest_size=digest_size)
    self.current: dict[int, int] = {}
    self.tolerable = ContextVar(tolerable)
    self.update(iter=chain(objs, iter))

  def copy(self) -> "ObjectHash":
    new = ObjectHash(tolerable=self.tolerable.value)
    new.hash = self.hash.copy()
    return new

  def hexdigest(self) -> str:
    return self.hash.hexdigest()

  __str__ = hexdigest

  def __eq__(self, value: object) -> bool:
    return isinstance(value, ObjectHash) and str(self) == str(value)

  def nested_hash(self, *objs: object) -> str:
    return ObjectHash(iter=objs, tolerable=self.tolerable.value).hexdigest()

  def write_bytes(self, *data: bytes, iter: Iterable[bytes] = ()) -> Self:
    for d in chain(data, iter):
      self.hash.update(d)
    return self

  def write_text(self, *data: str, iter: Iterable[str] = ()) -> Self:
    return self.write_bytes(iter=(d.encode() for d in chain(data, iter)))

  def header(self, *args: object) -> Self:
    return self.write_bytes(":".join(map(str, args)).encode())

  def update(self, *objs: object, iter: Iterable[object] = (), tolerable: bool | None=None, header: str | None = None) -> Self:
    with nc if tolerable is None else self.tolerable.set(tolerable):
      for obj in chain(objs, iter):
        if header is not None:
          self.write_bytes(header.encode())
          header = None
        try:
          self._update_one(obj)
        except Exception as ex:
          if self.tolerable.value:
            self.header("error").update(type(ex))
          else:
            raise ObjectHashError(obj, ex) from ex
      return self

  def _update_one(self, obj: object) -> None:
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
          header = "set"
          items = sorted(obj)
        except:
          header = "set-unsortable"
          items = sorted(map(self.nested_hash, obj))
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
        fn_file = Path(getfile(obj)).resolve()
        if fn_file.is_relative_to(stdlib):
          self.header("function-std", obj.__qualname__)
        else:
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
          self.update(obj.__reduce_ex__(PICKLE_PROTOCOL))
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
            case dict() | MappingProxyType():
              header = "dict"
              items = obj.items()
              if not isinstance(obj, OrderedDict):
                try:
                  items = sorted(items)
                except:
                  header = "dict-unsortable"
                  items = sorted((self.nested_hash(key), val) for key, val in items)
              self.header(header, encode_type_of(obj), len(obj)).update(iter=flatten(items))
            case _:
              self._update_object(obj)
        finally:
          del self.current[id(obj)]

  def _update_iterator(self, obj: Iterable) -> Self:
    return self.header("iterator", encode_type_of(obj)).update(iter=obj).header("iterator-end")

  def _update_object(self, obj: object) -> Self:
    self.header("instance", encode_type_of(obj))
    get_hash = hasattr(obj, "__objecthash__") and getattr(obj, "__objecthash__")
    if callable(get_hash):
      return self.header("objecthash").update(get_hash())
    reduced = None
    with suppress(Exception):
      reduced = obj.__reduce_ex__(PICKLE_PROTOCOL)
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
    if isinstance(obj, Iterable):
      return self._update_iterator(obj)
    repr_str = re.sub(r"\s+(at\s+0x[0-9a-fA-F]+)(>)$", r"\2", repr(obj))
    return self.header("repr").update(repr_str)

def get_fn_body(fn: Callable) -> str:
  try:
    source = inspect.getsource(fn)
  except OSError:
    return ""
  tokens = tokenize.generate_tokens(StringIO(source).readline)
  ignore_types = (tokenize.COMMENT, tokenize.NL)
  return "".join("\0" + token.string for token in tokens if token.type not in ignore_types)
