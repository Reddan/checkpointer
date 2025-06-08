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

When a `@checkpoint`-decorated function is called, `checkpointer` computes a unique identifier for the call. This identifier derives from the function's source code, its dependencies, captured variables, and the arguments passed.

It then tries to retrieve a cached result using this identifier. If a valid cached result is found, it's returned immediately. Otherwise, the original function executes, its result is stored, and then returned.

### 🚨 What Triggers Cache Invalidation?

`checkpointer` maintains cache correctness using two types of hashes:

#### 1. Function Identity Hash (One-Time per Function)

This hash represents the decorated function itself and is computed once (usually on first invocation). It covers:

* **Decorated Function's Code:**\
    The function's logic and signature (excluding parameter type annotations) are hashed. Formatting changes like whitespace, newlines, comments, or trailing commas do **not** cause invalidation.

* **Dependencies:**\
    All user-defined functions and methods the function calls or uses are included recursively. Dependencies are detected by:
    * Inspecting the function's global scope for referenced functions/objects.
    * Inferring from argument type annotations.
    * Analyzing object constructions and method calls to identify classes and methods used.

* **Top-Level Module Code:**\
    Changes unrelated to the function or its dependencies in the module do **not** trigger invalidation.

#### 2. Call Hash (Computed on Every Function Call)

Each function call's cache key (the **call hash**) combines:

* **Passed Arguments:**\
    Includes positional and keyword arguments, combined with default values. Changing defaults alone doesn't necessarily trigger invalidation unless it affects actual call values.

* **Captured Global Variables:**\
    When `capture=True` or explicit capture annotations are used, `checkpointer` hashes global variables referenced by the function:
    * `CaptureMe` variables are hashed on every call, so changes trigger invalidation.
    * `CaptureMeOnce` variables are hashed once per session for performance optimization.

* **Custom Argument Hashing:**\
    Using `HashBy` annotations, arguments or captured variables can be transformed before hashing (e.g., sorting lists to ignore order), allowing more precise or efficient call hashes.

## 💡 Usage

Once a function is decorated with `@checkpoint`, you can interact with its caching behavior using the following methods:

* **`expensive_function(...)`**:\
    Call the function normally. This will compute and cache the result or load it from cache.

* **`expensive_function.rerun(...)`**:\
    Force the original function to execute and overwrite any existing cached result.

* **`expensive_function.fn(...)`**:\
    Call the undecorated function directly, bypassing the cache (useful in recursion to prevent caching intermediate steps).

* **`expensive_function.get(...)`**:\
    Retrieve the cached result without executing the function. Raises `CheckpointError` if no valid cache exists.

* **`expensive_function.exists(...)`**:\
    Check if a cached result exists without computing or loading it.

* **`expensive_function.delete(...)`**:\
    Remove the cached entry for given arguments.

* **`expensive_function.reinit(recursive: bool = False)`**:\
    Recalculate the function identity hash and recapture `CaptureMeOnce` variables, updating the cached function state within the same Python session.

## ⚙️ Configuration & Customization

The `@checkpoint` decorator accepts the following parameters:

* **`storage`** (Type: `str` or `checkpointer.Storage`, Default: `"pickle"`)\
    Storage backend to use: `"pickle"` (disk-based, persistent), `"memory"` (in-memory, non-persistent), or a custom `Storage` class.

* **`directory`** (Type: `str` or `pathlib.Path` or `None`, Default: `~/.cache/checkpoints`)\
    Base directory for disk-based checkpoints (only for `"pickle"` storage).

* **`when`** (Type: `bool`, Default: `True`)\
    Enable or disable checkpointing dynamically, useful for environment-based toggling.

* **`capture`** (Type: `bool`, Default: `False`)\
    If `True`, includes global variables referenced by the function in call hashes (except those excluded via `NoHash`).

* **`should_expire`** (Type: `Callable[[datetime.datetime], bool]`, Default: `None`)\
    A custom callable that receives the `datetime` timestamp of a cached result. It should return `True` if the cached result is considered expired and needs recomputation, or `False` otherwise.

* **`fn_hash_from`** (Type: `Any`, Default: `None`)\
    Override the computed function identity hash with any hashable object you provide (e.g., version strings, config IDs). This gives you explicit control over the function's version and when its cache should be invalidated.

* **`verbosity`** (Type: `int` (`0`, `1`, or `2`), Default: `1`)\
    Controls the level of logging output from `checkpointer`.
    * `0`: No output.
    * `1`: Shows when functions are computed and cached.
    * `2`: Also shows when cached results are remembered (loaded from cache).

## 🔬 Customize Argument Hashing

You can customize how arguments are hashed without modifying the actual argument values to improve cache hit rates or speed up hashing.

* **`Annotated[T, HashBy[fn]]`**:\
    Transform the argument via `fn(argument)` before hashing. Useful for normalization (e.g., sorting lists) or optimized hashing for complex inputs.

* **`NoHash[T]`**:\
    Exclude the argument from hashing completely, so changes to it won't trigger cache invalidation.

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

In this example, the hash for `numbers` ignores order, `data_file` is hashed based on its contents rather than path, and changes to `log` don't affect caching.

## 🎯 Capturing Global Variables

`checkpointer` can include **captured global variables** in call hashes - these are globals your function reads during execution that may affect results.

Use `capture=True` on `@checkpoint` to capture **all** referenced globals (except those explicitly excluded with `NoHash`).

Alternatively, you can **opt-in selectively** by annotating globals with:

* **`CaptureMe[T]`**:\
    Capture the variable on every call (triggers invalidation on changes).

* **`CaptureMeOnce[T]`**:\
    Capture once per Python session (for expensive, immutable globals).

You can also combine these with `HashBy` to customize how captured variables are hashed (e.g., hash by subset of attributes).

**Example:**

```python
from typing import Annotated
from checkpointer import checkpoint, CaptureMe, CaptureMeOnce, HashBy
from pathlib import Path

def file_bytes(path: Path) -> bytes:
    return path.read_bytes()

captured_data: CaptureMe[Annotated[Path, HashBy[file_bytes]]] = Path("data.txt")
session_config: CaptureMeOnce[dict] = {"mode": "prod"}

@checkpoint
def process():
    # `captured_data` is included in the call hash on every call, hashed by file content
    # `session_config` is hashed once per session
    ...
```

## 🗄️ Custom Storage Backends

Implement your own storage backend by subclassing `checkpointer.Storage` and overriding required methods.

Within storage methods, `call_hash` identifies calls by arguments. Use `self.fn_id()` to get function identity (name + hash/version), important for organizing checkpoints.

**Example:**

```python
from checkpointer import checkpoint, Storage
from datetime import datetime

class MyCustomStorage(Storage):
    def exists(self, call_hash):
        fn_dir = self.checkpointer.directory / self.fn_id()
        return (fn_dir / call_hash).exists()

    def store(self, call_hash, data):
        ...  # Store serialized data
        return data  # Must return data to checkpointer

    def checkpoint_date(self, call_hash): ...
    def load(self, call_hash): ...
    def delete(self, call_hash): ...

@checkpoint(storage=MyCustomStorage)
def custom_cached_function(x: int):
    return x ** 2
```

## ⚡ Async Support

`checkpointer` works with Python's `asyncio` and other async runtimes.

```python
import asyncio
from checkpointer import checkpoint

@checkpoint
async def async_compute_sum(a: int, b: int) -> int:
    print(f"Asynchronously computing {a} + {b}...")
    await asyncio.sleep(1)
    return a + b

async def main():
    result1 = await async_compute_sum(3, 7)
    print(f"Result 1: {result1}")

    result2 = await async_compute_sum(3, 7)
    print(f"Result 2: {result2}")

    result3 = async_compute_sum.get(3, 7)
    print(f"Result 3 (from cache): {result3}")

asyncio.run(main())
```
