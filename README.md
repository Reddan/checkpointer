# checkpointer · [![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Reddan/checkpointer/blob/master/LICENSE) [![pypi](https://img.shields.io/pypi/v/checkpointer)](https://pypi.org/project/checkpointer/) [![pypi](https://img.shields.io/pypi/pyversions/checkpointer)](https://pypi.org/project/checkpointer/)

`checkpointer` is a Python library for memoizing function results with **code-aware cache invalidation**. Decorate a function with `@checkpoint` and its return values are cached to disk (or memory). When you call it again with the same arguments, the cached result is returned instead of recomputing it.

What makes it different from ordinary memoization is that the cache invalidates itself automatically when your **code** changes - not just when arguments change. Edit a function's logic, or the logic of anything it depends on, and the stale cache is discarded on the next run. You get the speed of caching without the classic footgun of serving results from code that no longer exists.

It works with sync and async functions, methods, and recursion, handles complex objects and large **NumPy** / **PyTorch** arrays, and lets you fine-tune exactly what counts toward a cache key.

## 📦 Installation

```bash
pip install checkpointer
```

Requires Python 3.11+. No mandatory dependencies. NumPy, PyTorch, and Polars are supported automatically if they happen to be installed.

## 🚀 Quick Start

```python
from checkpointer import checkpoint

@checkpoint
def load_dataset(path: str) -> pl.DataFrame:
    print("Reading and parsing...")
    return pl.read_csv(path).filter(pl.col("price") > 0)

df = load_dataset("sales.csv")  # Reads, parses, and caches the DataFrame
df = load_dataset("sales.csv")  # Skips the work - loaded from cache
```

The win shows up in everyday iteration: a script or notebook that reloads a multi-second dataset on every run reads it once and reuses the result on subsequent runs. And because the cache is *code-aware*, the moment you change `load_dataset` (say, tighten the filter), the stale result is dropped and the file is re-parsed - no manual cache-busting.

By default, results are pickled to `~/.cache/checkpoints`, so the cache survives across processes and restarts. (Polars `DataFrame`s are stored as Parquet automatically.)

## 🧠 How It Works

Every cached call is identified by two hashes:

- **Function identity hash** - computed once per function (on first use). It captures the function's *source code* and the source of every user-defined function, method, and class it depends on, recursively. Change any of that logic and the hash changes, invalidating all cached results for that function. Cosmetic edits (comments, whitespace, formatting, type annotations) are deliberately ignored.
- **Call hash** - computed on every call from the actual arguments (and, optionally, captured global variables). Different arguments produce different call hashes.

When you call a decorated function, `checkpointer` combines these into a lookup key. If a valid cached result exists, it's returned immediately; otherwise the function runs, the result is stored, and then returned.

Because dependency tracking is automatic, you rarely need to bump a version number by hand - editing the code *is* the version bump.

### What counts as a dependency

The identity hash follows your function into the code it actually uses. `checkpointer` discovers dependencies by:

- **Inspecting the global scope** - functions, methods, and classes the function references are pulled in (recursively, including their dependencies).
- **Inferring from type annotations** - classes named in argument annotations are treated as dependencies, so changes to their methods invalidate the cache too.
- **Analyzing constructions and calls** - objects built and methods invoked inside the function are traced back to the classes and methods they come from.

A few things deliberately **don't** invalidate:

- Cosmetic edits - comments, whitespace, formatting, and parameter type annotations.
- Changes elsewhere in the module that the function doesn't touch.
- Changing a parameter's default value, unless it changes the actual arguments a call resolves to.

## 💡 Examples

### Async functions

Works with any async runtime - the awaited value is what gets cached, so repeated calls skip the network entirely.

```python
@checkpoint
async def fetch_profile(user_id: int) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"https://api.example.com/users/{user_id}")
        return resp.json()

profile = await fetch_profile(42)  # Hits the API
profile = await fetch_profile(42)  # Instant - from cache
```

### Methods

Decorate methods directly. The instance is hashed as part of the call, so results are keyed to it - two embedders with different models cache separately, with no collisions.

Two things make this example efficient: the method returns the NumPy array **as-is** (don't `.tolist()` it - `checkpointer` pickles arrays compactly and far faster than a Python list), and the class defines `__objecthash__` so hashing an instance is instant instead of crawling the whole loaded model. See [Custom Instance Hashing](#custom-instance-hashing-with-__objecthash__) for the details.

```python
import numpy as np
from sentence_transformers import SentenceTransformer

class Embedder:
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)  # Loaded once, reused

    def __objecthash__(self):
        return self.model_name  # Fast, stable identity - skips hashing the model

    @checkpoint
    def embed(self, text: str) -> np.ndarray:
        return self.model.encode(text)  # Cached as a NumPy array

fast = Embedder("all-MiniLM-L6-v2")
fast.embed("hello world")  # Computed and cached for this model
fast.embed("hello world")  # From cache - the model isn't even consulted
```

### Force recomputation

`.rerun(...)` runs the function and overwrites the cache - useful when an upstream data source changed but your code didn't.

```python
df = load_dataset("sales.csv")          # Cached
df = load_dataset.rerun("sales.csv")    # Recomputes and overwrites the cache
```

### Expiry / TTL

Expire results by age with a `timedelta`, or by a custom rule with a callable that receives the store timestamp and returns `True` when stale.

```python
from datetime import datetime, timedelta

# Re-fetch a volatile rate at most once every 15 minutes
@checkpoint(expiry=timedelta(minutes=15))
def get_exchange_rate(base: str, quote: str) -> float:
    return httpx.get(f"https://api.example.com/rate/{base}/{quote}").json()["rate"]

# Invalidate anything cached before today's UTC midnight
@checkpoint(expiry=lambda stored_at: stored_at.date() < datetime.utcnow().date())
def daily_report(team: str) -> dict: ...
```

### Layered / multi-backend caching

Stack decorators to combine backends - e.g. a fast in-memory layer in front of a persistent disk layer - without losing cache consistency. Great for a lookup hit many times per run that's also worth keeping across runs.

```python
@checkpoint(storage="memory")  # Hot path, in-process
@checkpoint(storage="pickle")  # Persistent, on disk
def geocode(address: str) -> tuple[float, float]:
    resp = httpx.get("https://api.example.com/geocode", params={"q": address})
    return tuple(resp.json()["latlng"])

geocode("1600 Amphitheatre Pkwy")     # API call, written to both layers
geocode("1600 Amphitheatre Pkwy")     # From memory
geocode.fn.get("1600 Amphitheatre Pkwy")  # From the pickle layer underneath
```

### Toggle caching on/off

Flip caching with `when` - keep the persisted cache while iterating locally, but run clean in production (or in tests).

```python
import os

IS_DEV = os.environ.get("ENV") == "dev"

@checkpoint(when=IS_DEV)  # Caches while developing; runs straight through otherwise
def build_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(...)  # expensive feature engineering
```

### Recursion

Inside a recursive function, call `.fn(...)` to invoke the original, undecorated function. This caches the top-level result without writing a separate checkpoint for every intermediate step - handy when the recursion fans out over expensive calls.

```python
@checkpoint
def resolve_deps(package: str) -> set[str]:
    deps = fetch_dependencies(package)  # e.g. a registry API call
    return deps | {sub for dep in deps for sub in resolve_deps.fn(dep)}

resolve_deps("flask")        # Caches the fully-resolved dependency set
resolve_deps.get("flask")    # Reads it back; transitive deps weren't cached individually
```

## Customizing How Arguments Are Hashed

Control what an argument contributes to the call hash - without changing the value the function actually receives. Useful for normalization (better hit rates) or for hashing something cheaper/more meaningful than the raw object.

- **`Annotated[T, HashBy[fn]]`** - hash `fn(arg)` instead of `arg`.
- **`NoHash[T]`** - exclude the argument from the hash entirely.

```python
from typing import Annotated
from pathlib import Path
import logging
from checkpointer import checkpoint, HashBy, NoHash

def file_bytes(path: Path) -> bytes:
    return path.read_bytes()

@checkpoint
def process(
    numbers: Annotated[list[int], HashBy[sorted]],   # Order-insensitive
    data_file: Annotated[Path, HashBy[file_bytes]],  # Hash by file contents, not path
    log: NoHash[logging.Logger],                     # Ignored entirely
):
    ...
```

Here `[3, 1, 2]` and `[1, 2, 3]` hit the same cache entry, the cache tracks the file's *contents* rather than its name, and swapping loggers never invalidates anything.

## Custom Instance Hashing with `__objecthash__`

Any class can define `__objecthash__` to control how its instances are hashed. When `checkpointer` encounters an instance, it hashes the return value of `__objecthash__()` instead of inspecting the object's internals.

```python
class Model:
    def __init__(self, id: str, weights: list[float]):
        self.id = id
        self.weights = weights

    def __objecthash__(self):
        return self.id  # Identity depends only on `id`
```

The return value can be anything `checkpointer` knows how to hash - a string, tuple, dict, etc. Once defined, it applies everywhere the class appears: as an argument, a captured variable, or nested inside another value - no per-call-site annotation needed.

## Capturing Global Variables

Sometimes a function's result depends on a module-level global, not just its arguments. `checkpointer` can fold such **captured globals** into the call hash so the cache invalidates when they change.

Enable it broadly with `capture=True` (captures every referenced global except those marked `NoHash`), or opt in per-variable with annotations:

- **`CaptureMe[T]`** - hashed on *every* call; changes invalidate immediately.
- **`CaptureMeOnce[T]`** - hashed *once per Python session*; cheaper, for expensive immutable globals.

Both combine with `HashBy` to customize hashing.

```python
from typing import Annotated
from pathlib import Path
from checkpointer import checkpoint, CaptureMe, CaptureMeOnce, HashBy

def file_bytes(path: Path) -> bytes:
    return path.read_bytes()

config_file: CaptureMe[Annotated[Path, HashBy[file_bytes]]] = Path("config.yaml")
session_seed: CaptureMeOnce[int] = 42

@checkpoint
def run():
    # Re-hashes `config_file` (by contents) every call;
    # hashes `session_seed` once per session.
    ...
```

## Custom Storage Backends

Beyond the built-in `"pickle"` and `"memory"` backends, you can implement your own - e.g. to cache in Redis, S3, or a database. Subclass `Storage`, implement a handful of methods, and pass the class as `storage`. Calls are identified by `call_hash`; use `self.fn_id()` to namespace entries by function identity (name + version hash).

```python
from checkpointer import checkpoint, Storage

class RedisStorage(Storage):
    def store(self, call_hash, data):
        redis.set(self._key(call_hash), pickle.dumps(data))
        return data  # must return data
    def load(self, call_hash):
        return pickle.loads(redis.get(self._key(call_hash)))
    def exists(self, call_hash):
        return bool(redis.exists(self._key(call_hash)))
    # ...plus delete() and checkpoint_date()

@checkpoint(storage=RedisStorage)
def cached(x: int):
    return x ** 2
```

See [the `Storage` interface](#custom-storage-interface) in the API reference for the complete set of methods.

---

# 📚 API Reference

## `@checkpoint`

The default decorator. Also available as a configurable factory - call it with options to get a new, reusable checkpointer:

```python
@checkpoint                       # use defaults
@checkpoint(storage="memory")     # override options
dev = checkpoint(when=IS_DEV)     # reusable preset
```

### Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `storage` | `"pickle"` \| `"memory"` \| `type[Storage]` | `"pickle"` | Backend. `"pickle"` is persistent on disk; `"memory"` lives in-process; or pass a custom `Storage` subclass. |
| `directory` | `str` \| `Path` \| `None` | `~/.cache/checkpoints` | Root directory for the `"pickle"` backend. |
| `capture` | `bool` | `False` | If `True`, include all referenced globals in call hashes (except those marked `NoHash`). |
| `expiry` | `timedelta` \| `Callable[[datetime], bool]` \| `None` | `None` | Treat a cached result as stale. A `timedelta` expires by age; a callable receives the store timestamp and returns `True` when expired. |
| `fn_hash_from` | `Any` | `None` | Override the computed function-identity hash with any hashable value (a version string, config id, etc.). Set this and source-code changes no longer auto-invalidate - *you* control the version. |
| `when` | `bool` | `True` | Master on/off switch. When `False`, calls run straight through with no caching. |
| `verbosity` | `0` \| `1` \| `2` | `1` | `0`: silent. `1`: log on compute/store. `2`: also log on cache hits. |

## `CachedFunction` methods

A decorated function becomes a `CachedFunction`. Calling it normally caches or loads; the following give finer control. (`*args, **kw` below are always the function's own arguments.)

| Member | Description |
| --- | --- |
| `fn(*args, **kw)` | The original, undecorated function (a property). Bypasses the cache - use it in recursion. |
| `rerun(*args, **kw)` | Force execution and overwrite any cached result. |
| `cached(*args, **kw)` | Like calling normally, but ignores `when=False` (always uses the cache). |
| `get(*args, **kw)` | Return the cached result without computing. Raises `CheckpointError` if absent. |
| `get_or(default, *args, **kw)` | Like `get`, but returns `default` instead of raising. |
| `set(value, *args, **kw)` | Manually store `value` as the result for these arguments. Use this for a **sync** function, whose return value is stored directly. |
| `set_awaitable(value, *args, **kw)` | The `set` for an **async** function, whose resolved value is stored wrapped so that loading it yields an awaitable (matching the original signature) - the wrapping is handled for you. |
| `exists(*args, **kw)` | `True` if a cached entry exists for these arguments. |
| `delete(*args, **kw)` | Remove the cached entry for these arguments. |
| `get_call_hash(*args, **kw)` | The call hash these arguments produce. |
| `is_expired(call_hash)` | `True` if no entry exists or it has expired per `expiry`. |
| `reinit(recursive=True)` | Recompute the function-identity hash and re-capture `CaptureMeOnce` globals within the current session. |
| `cleanup(invalidated=True, expired=True)` | Delete checkpoints from outdated function versions and/or expired entries. |
| `ident` | The `FunctionIdent` - exposes `fn_hash`, dependencies, and capturables. |
| `storage` | The bound `Storage` instance. |

## Annotations & types

Importable from `checkpointer`:

- `HashBy[fn]` - used as `Annotated[T, HashBy[fn]]`; hash by `fn(value)`.
- `NoHash[T]` - exclude a value from hashing (alias for `Annotated[T, HashBy[to_none]]`).
- `CaptureMe[T]` - capture a global into the call hash on every call.
- `CaptureMeOnce[T]` - capture a global once per session.
- `AwaitableValue` - the internal wrapper for async results. You normally never touch it; reach for `set_awaitable` instead of constructing one by hand.
- `CachedFunction`, `Checkpointer`, `FunctionIdent` - core types.
- `CheckpointError` - raised by `get` when no valid cache exists.
- `Storage`, `PickleStorage`, `MemoryStorage` - storage backends.
- `ObjectHash` - the hashing engine (handles arbitrary objects, NumPy/PyTorch arrays, circular references, and `__objecthash__`).

## Pre-configured checkpointers

Ready-made presets, importable from `checkpointer`:

| Name | Equivalent to |
| --- | --- |
| `checkpoint` | `Checkpointer()` - disk-backed defaults. |
| `capture_checkpoint` | `Checkpointer(capture=True)` - captures all referenced globals. |
| `memory_checkpoint` | `Checkpointer(storage="memory", verbosity=0)` - in-process, silent. |
| `tmp_checkpoint` | `Checkpointer(directory="<tmp>/checkpoints")` - stored in the system temp dir. |
| `static_checkpoint` | `Checkpointer(fn_hash_from=())` - disables code-aware invalidation; the identity hash is fixed until you change `fn_hash_from`. |

## Module-level functions

- `cleanup_all(invalidated=True, expired=True)` - run `cleanup` on every live `CachedFunction`.
- `cleanup_memory_storage()` - drop in-memory checkpoints for functions that no longer exist.
- `get_function_hash(fn)` - compute a function's identity hash without decorating it.

## Custom `Storage` interface

Subclass `Storage` and implement the methods below. The base class provides `fn_id()`, `fn_dir()`, and expiry helpers.

```python
class Storage:
    checkpointer: Checkpointer
    cached_fn: CachedFunction

    def store(self, call_hash, data) -> Any: ...   # persist & return data
    def exists(self, call_hash) -> bool: ...
    def load(self, call_hash) -> Any: ...
    def delete(self, call_hash) -> None: ...
    def checkpoint_date(self, call_hash) -> datetime: ...
    def cleanup(self, invalidated=True, expired=True) -> None: ...
    def clear(self) -> None: ...
```

The `"pickle"` backend additionally serializes Polars `DataFrame`s as Parquet when Polars is installed.
