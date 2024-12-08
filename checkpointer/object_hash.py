import hashlib
import inspect
import tokenize
import numpy as np
from typing import Any, TypeVar, Callable
from io import StringIO
from collections.abc import Iterable, Mapping
from decimal import Decimal
from pickle import HIGHEST_PROTOCOL as PROTOCOL
from types import BuiltinFunctionType, FunctionType, MethodType, ModuleType, UnionType, GeneratorType

def encode_type(t: type) -> str:
  return f"{t.__module__}:{t.__qualname__}"

def encode_val(v: Any, *args: Any) -> str:
  return ":".join(map(str, ("val", encode_type(type(v))) + args))

def make_c_contiguous(array: np.ndarray) -> np.ndarray:
  is_sliced = array.nbytes != array.size * array.itemsize
  if array.flags.f_contiguous and not array.flags.c_contiguous:
    array = array.T
  if array.flags.c_contiguous or is_sliced:
    return np.ascontiguousarray(array)
  strides = tuple(array.itemsize * np.cumprod((1,) + array.shape[::-1][:-1])[::-1])
  return np.lib.stride_tricks.as_strided(array, strides=strides)

def get_fn_body(fn: Callable) -> str:
  try:
    source = inspect.getsource(fn)
  except OSError:
    return ""
  tokens = tokenize.generate_tokens(StringIO(source).readline)
  ignore_types = (tokenize.COMMENT, tokenize.NL)
  return "".join("\0" + token.string for token in tokens if token.type not in ignore_types)

class ObjectHashError(Exception):
  def __init__(self, obj: Any, cause: Exception):
    super().__init__(f"{type(cause).__name__} error when hashing {obj}")
    self.obj = obj

class ObjectHash:
  def __init__(self, *obj: Any, digest_size=64, tolerate_errors=False) -> None:
    self.hash = hashlib.blake2b(digest_size=digest_size)
    self.current: dict[int, int] = {}
    self.tolerate_errors = tolerate_errors
    self.update(*obj)

  def copy(self) -> "ObjectHash":
    new = ObjectHash(tolerate_errors=self.tolerate_errors)
    new.hash = self.hash.copy()
    return new

  def hexdigest(self) -> str:
    return self.hash.hexdigest()

  __str__ = hexdigest

  def update_hash(self, *data: bytes | str) -> None:
    for d in data:
      self.hash.update(d.encode() if isinstance(d, str) else d)

  def update(self, *objs: Any) -> "ObjectHash":
    for obj in objs:
      try:
        self._update_one(obj)
      except Exception as ex:
        if self.tolerate_errors:
          self.hash.update(b"error")
          self.update(type(ex))
          continue
        raise ObjectHashError(obj, ex) from ex
    return self

  def _update_one(self, obj: Any) -> None:
    match obj:
      case None:
        self.update_hash(b"null")

      case bool() | int() | float() | complex() | Decimal() | ObjectHash():
        self.update_hash(encode_val(obj, obj))

      case str() | bytes() | bytearray() | memoryview():
        self.update_hash(encode_val(obj, len(obj)))
        self.update_hash(obj)

      case set() | frozenset():
        self.update_hash(encode_val(obj, len(obj)))
        try:
          items = sorted(obj)
        except:
          items = sorted(str(ObjectHash(item, tolerate_errors=self.tolerate_errors)) for item in obj)
          self.update_hash("unsortable")
        self.update(*items)

      case TypeVar():
        self.update_hash("TypeVar")
        self.update(obj.__name__, obj.__bound__, obj.__constraints__, obj.__contravariant__, obj.__covariant__)

      case UnionType():
        self.update_hash("UnionType")
        self.update(obj.__args__)

      case np.dtype():
        self.update(obj.__class__, obj.descr)

      case np.ndarray():
        self.update_hash(encode_val(obj, obj.shape, obj.strides))
        self.update(obj.dtype)
        if obj.dtype.hasobject:
          self.update(obj.__reduce_ex__(PROTOCOL))
        else:
          self.update_hash(make_c_contiguous(obj).view(np.uint8).data)

      case BuiltinFunctionType():
        self.update_hash(f"builtin:{obj.__name__}")

      case FunctionType():
        self.update_hash(f"function:{obj.__module__}:{obj.__qualname__}")
        self.update_hash(get_fn_body(obj))
        if obj.__defaults__:
          self.update("defaults", obj.__defaults__)
        if obj.__kwdefaults__:
          self.update("kwdefaults", obj.__kwdefaults__)
        if obj.__annotations__:
          self.update("annotations", obj.__annotations__)

      case MethodType():
        self.update("method", obj.__func__, obj.__self__.__class__)

      case type():
        self.update_hash(f"type:{encode_type(obj)}")

      case ModuleType():
        self.update_hash(f"module:{obj.__name__}:{obj.__file__}")

      case GeneratorType():
        self.update_hash(f"generator:{obj.__qualname__}")
        self.update_iterator(obj)

      case _ if id(obj) in self.current:
        self.update_hash(f"circular:{self.current[id(obj)]}")

      case _:
        self.current[id(obj)] = len(self.current)
        match obj:
          case list() | tuple():
            self.update_hash(encode_val(obj, len(obj)))
            self.update(*obj)

          case dict():
            try:
              items = sorted(obj.items())
            except:
              items = sorted((str(ObjectHash(key, tolerate_errors=self.tolerate_errors)), val) for key, val in obj.items())
            self.update_hash(encode_val(obj, len(obj)))
            for key, value in items:
              self.update(key, value)

          case _:
            self.update_object(obj)

        del self.current[id(obj)]

  def update_iterator(self, obj: Iterable) -> None:
    self.update_hash(f"iterator:{encode_type(type(obj))}")
    for i, v in enumerate(obj):
      self.update_hash(f"i:{i}")
      self.update(v)

  def update_object(self, obj: object) -> None:
    self.update_hash(f"instance:{type(obj).__module__}:{type(obj).__qualname__}")
    if hasattr(obj, "__reduce_ex__") or hasattr(obj, "__reduce__"):
      reduced = obj.__reduce_ex__(PROTOCOL) if hasattr(obj, "__reduce_ex__") else obj.__reduce__()
      if isinstance(reduced, str):
        return self.update_hash(f"reduce-str:{reduced}")
      reduced = list(reduced)
      it = reduced.pop(3) if len(reduced) >= 4 else None
      self.update_hash("reduce")
      self.update(reduced)
      if it is not None:
        self.update_iterator(it)
    elif hasattr(obj, "__getstate__"):
      self.update("getstate", obj.__getstate__())
    elif hasattr(obj, "__slots__"):
      self.update("slots", {slot: getattr(obj, slot) for slot in getattr(obj, "__slots__")})
    elif hasattr(obj, "__dict__"):
      self.update("dict", obj.__dict__)
    else:
      self.update_hash(f"custom:{repr(obj)}")
