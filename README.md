# checkpointer · [![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Reddan/checkpointer/blob/master/LICENSE) [![pypi](https://img.shields.io/pypi/v/checkpointer)](https://pypi.org/project/checkpointer/) [![pypi](https://img.shields.io/pypi/pyversions/checkpointer)](https://pypi.org/project/checkpointer/)

`checkpointer` is a Python library offering a decorator-based API for memoizing (caching) function results with code-aware cache invalidation. It works with sync and async functions, supports multiple storage backends, and refreshes caches automatically when your code or dependencies change - helping you maintain correctness, speed up execution, and smooth out your workflows by skipping redundant, costly operations.

## 📦 Installation

```bash
pip install checkpointer
```

## 🚀 Quick Start

Apply the `@checkpoint` decorator to any function:

```python
from checkpointer import checkpoint

@checkpoint
def expensive_function(x: int) -> int:
    print("Computing...")
    return x ** 2

result = expensive_function(4)  # Computes and stores the result
result = expensive_function(4)  # Loads from the cache
```

## 🧠 How It Works

When a `@checkpoint`-decorated function is called, `checkpointer` first computes a unique identifier (hash) for the call. This hash is derived from the function's source code, its dependencies, and the arguments passed.

It then tries to retrieve a cached result using this ID. If a valid cached result is found, it's returned immediately. Otherwise, the original function executes, its result is stored, and then returned.

Cache validity is determined by this function's hash, which automatically updates if:

* **Function Code Changes**: The decorated function's source code is modified.
* **Dependencies Change**: Any user-defined function in its dependency tree (direct or indirect, even across modules or not decorated) is modified.
* **Captured Variables Change** (with `capture=True`): Global or closure-based variables used within the function are altered.

**Example: Dependency Invalidation**

```python
def multiply(a, b):
    return a * b

@checkpoint
def helper(x):
    # Depends on `multiply`
    return multiply(x + 1, 2)

@checkpoint
def compute(a, b):
    # Depends on `helper` and `multiply`
    return helper(a) + helper(b)
```

If `multiply` is modified, caches for both `helper` and `compute` are automatically invalidated and recomputed.

## 💡 Usage

Once a function is decorated with `@checkpoint`, you can interact with its caching behavior using the following methods:

* **`expensive_function(...)`**:\
    Call the function normally. This will either compute and cache the result or load it from the cache if available.

* **`expensive_function.rerun(...)`**:\
    Forces the original function to execute, compute a new result, and overwrite any existing cached value for the given arguments.

* **`expensive_function.fn(...)`**:\
    Calls the original, undecorated function directly, bypassing the cache entirely. This is particularly useful within recursive functions to prevent caching intermediate steps.

* **`expensive_function.get(...)`**:\
    Attempts to retrieve the cached result for the given arguments without executing the original function. Raises `CheckpointError` if no valid cached result exists.

* **`expensive_function.exists(...)`**:\
    Checks if a cached result exists for the given arguments without attempting to compute or load it. Returns `True` if a valid checkpoint exists, `False` otherwise.

* **`expensive_function.delete(...)`**:\
    Removes the cached entry for the specified arguments.

* **`expensive_function.reinit()`**:\
    Recalculates the function's internal hash. This is primarily used when `capture=True` and you need to update the cache based on changes to external variables within the same Python session.

## ⚙️ Configuration & Customization

The `@checkpoint` decorator accepts the following parameters to customize its behavior:

* **`format`** (Type: `str` or `checkpointer.Storage`, Default: `"pickle"`)\
    Defines the storage backend to use. Built-in options are `"pickle"` (disk-based, persistent) and `"memory"` (in-memory, non-persistent). You can also provide a custom `Storage` class.

* **`root_path`** (Type: `str` or `pathlib.Path` or `None`, Default: `~/.cache/checkpoints`)\
    The base directory for storing disk-based checkpoints. This parameter is only relevant when `format` is set to `"pickle"`.

* **`when`** (Type: `bool`, Default: `True`)\
    A boolean flag to enable or disable checkpointing for the decorated function. This is particularly useful for toggling caching based on environment variables (e.g., `when=os.environ.get("ENABLE_CACHING", "false").lower() == "true"`).

* **`capture`** (Type: `bool`, Default: `False`)\
    If set to `True`, `checkpointer` includes global or closure-based variables used by the function in its hash calculation. This ensures that changes to these external variables also trigger cache invalidation and recomputation.

* **`should_expire`** (Type: `Callable[[datetime.datetime], bool]`, Default: `None`)\
    A custom callable that receives the `datetime` timestamp of a cached result. It should return `True` if the cached result is considered expired and needs recomputation, or `False` otherwise.

* **`fn_hash_from`** (Type: `Any`, Default: `None`)\
    This allows you to override the automatically computed function hash, giving you explicit control over when the function's cache should be invalidated. You can pass any object relevant to your invalidation logic (e.g., version strings, config parameters). The object you provide will be hashed internally by `checkpointer`.

* **`verbosity`** (Type: `int` (`0`, `1`, or `2`), Default: `1`)\
    Controls the level of logging output from `checkpointer`.
    * `0`: No output.
    * `1`: Shows when functions are computed and cached.
    * `2`: Also shows when cached results are remembered (loaded from cache).

## 🔬 Customize Argument Hashing

You can customize how individual function arguments are hashed without changing their actual values when passed in.

* **`Annotated[T, HashBy[fn]]`**:\
    Hashes the argument by applying `fn(argument)` before hashing. This enables custom normalization (e.g., sorting lists to ignore order) or optimized hashing for complex types, improving cache hit rates or speeding up hashing.

* **`NoHash[T]`**:\
    Completely excludes the argument from hashing, so changes to it won’t trigger cache invalidation.

**Example:**

```python
from typing import Annotated
from checkpointer import checkpoint, HashBy, NoHash
from pathlib import Path
import logging

def file_bytes(path: Path) -> bytes:
    return path.read_bytes()

@checkpoint
def process(
    numbers: Annotated[list[int], HashBy[sorted]],   # Hash by sorted list
    data_file: Annotated[Path, HashBy[file_bytes]],  # Hash by file content
    log: NoHash[logging.Logger],                     # Exclude logger from hashing
):
    ...
```

In this example, the cache key for `numbers` ignores order, `data_file` is hashed based on its contents rather than path, and changes to `log` don’t affect caching.

## 🗄️ Custom Storage Backends

For integration with databases, cloud storage, or custom serialization, implement your own storage backend by inheriting from `checkpointer.Storage` and implementing its abstract methods.

Within custom storage methods, `call_hash` identifies calls by arguments. Use `self.fn_id()` to get the function's unique identity (name + hash/version), crucial for organizing stored checkpoints (e.g., by function version). Access global `Checkpointer` config via `self.checkpointer`.

**Example: Custom Storage Backend**

```python
from checkpointer import checkpoint, Storage
from datetime import datetime

class MyCustomStorage(Storage):
    def exists(self, call_hash):
        # Example: Constructing a path based on function ID and call ID
        fn_dir = self.checkpointer.root_path / self.fn_id()
        return (fn_dir / call_hash).exists()

    def store(self, call_hash, data):
        ... # Store the serialized data for `call_hash`
        return data # This method must return the data back to checkpointer

    def checkpoint_date(self, call_hash): ...
    def load(self, call_hash): ...
    def delete(self, call_hash): ...

@checkpoint(format=MyCustomStorage)
def custom_cached_function(x: int):
    return x ** 2
```

## 🧱 Layered Caching

You can apply multiple `@checkpoint` decorators to a single function to create layered caching strategies. `checkpointer` processes these decorators from bottom to top, meaning the decorator closest to the function definition is evaluated first.

This is useful for scenarios like combining a fast, ephemeral cache (e.g., in-memory) with a persistent, slower cache (e.g., disk-based).

**Example: Memory Cache over Disk Cache**

```python
from checkpointer import checkpoint

@checkpoint(format="memory") # Layer 2: Fast, ephemeral in-memory cache
@checkpoint(format="pickle") # Layer 1: Persistent disk cache
def some_expensive_operation():
    print("Performing a time-consuming operation...")
    return sum(i for i in range(10**7))
```

## ⚡ Async Support

`checkpointer` works seamlessly with Python's `asyncio` and other async runtimes.

```python
import asyncio
from checkpointer import checkpoint

@checkpoint
async def async_compute_sum(a: int, b: int) -> int:
    print(f"Asynchronously computing {a} + {b}...")
    await asyncio.sleep(1)
    return a + b

async def main():
    # First call computes and caches
    result1 = await async_compute_sum(3, 7)
    print(f"Result 1: {result1}")

    # Second call loads from cache
    result2 = await async_compute_sum(3, 7)
    print(f"Result 2: {result2}")

    # Retrieve from cache without re-running the async function
    result3 = async_compute_sum.get(3, 7)
    print(f"Result 3 (from cache): {result3}")

asyncio.run(main())
```
