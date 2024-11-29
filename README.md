# checkpointer &middot; [![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Reddan/checkpointer/blob/master/LICENSE) [![pypi](https://img.shields.io/pypi/v/checkpointer)](https://pypi.org/project/checkpointer/) [![Python 3.12](https://img.shields.io/badge/python-3.12-blue)](https://pypi.org/project/checkpointer/)

`checkpointer` is a Python library for memoizing function results. It provides a decorator-based API with support for multiple storage backends. Use it for computationally expensive operations where caching can save time, or during development to avoid waiting for redundant computations.

Adding or removing `@checkpoint` doesn't change how your code works. You can apply it to any function, including ones you've already written, without altering their behavior or introducing side effects. The original function remains unchanged and can still be called directly when needed.

### Key Features:
- 🗂️ **Multiple Storage Backends**: Built-in support for in-memory and pickle-based storage, or create your own.
- 🎯 **Simple Decorator API**: Apply `@checkpoint` to functions without boilerplate.
- 🔄 **Async and Sync Compatibility**: Works with synchronous functions and any Python async runtime (e.g., `asyncio`, `Trio`, `Curio`).
- ⏲️ **Custom Expiration Logic**: Automatically invalidate old checkpoints.
- 📂 **Flexible Path Configuration**: Control where checkpoints are stored.
- 📦 **Captured Variables Handling**: Optionally include captured variables in cache invalidation.

---

## Installation

```bash
pip install checkpointer
```

---

## Quick Start 🚀

```python
from checkpointer import checkpoint

@checkpoint
def expensive_function(x: int) -> int:
    print("Computing...")
    return x ** 2

result = expensive_function(4)  # Computes and stores the result
result = expensive_function(4)  # Loads from the cache
```

---

## How It Works

When you use `@checkpoint`, the function's **arguments** (`args`, `kwargs`) are hashed to create a unique identifier for each call. This identifier is used to store and retrieve cached results. If the same arguments are passed again, `checkpointer` loads the cached result instead of recomputing.

Additionally, `checkpointer` ensures that caches are invalidated when a function's implementation or any of its dependencies change. Each function is assigned a hash based on:

1. **Its source code**: Changes to the function's code update its hash.
2. **Dependent functions**: If a function calls others, changes in those dependencies will also update the hash.
3. **Captured variables**: (Optional) If `capture=True`, changes to captured variables and global variables will also update the hash.

### Example: Cache Invalidation

```python
def multiply(a, b):
    return a * b

@checkpoint
def helper(x):
    return multiply(x + 1, 2)

@checkpoint
def compute(a, b):
    return helper(a) + helper(b)
```

If you modify `multiply`, caches for both `helper` and `compute` are invalidated and recomputed.

---

## Parameterization

### Custom Configuration

Set up a `Checkpointer` instance with custom settings, and extend it by calling itself with overrides:

```python
from checkpointer import checkpoint

IS_DEVELOPMENT = True  # Toggle based on your environment

tmp_checkpoint = checkpoint(root_path="/tmp/checkpoints")
dev_checkpoint = tmp_checkpoint(when=IS_DEVELOPMENT)  # Adds development-specific behavior
```

### Per-Function Customization & Layered Caching

Layer caches by stacking checkpoints:

```python
@checkpoint(format="memory")  # Always use memory storage
@dev_checkpoint  # Adds caching during development
def some_expensive_function():
    print("Performing a time-consuming operation...")
    return sum(i * i for i in range(10**6))
```

- **In development**: Both `dev_checkpoint` and `memory` caches are active.
- **In production**: Only the `memory` cache is active.

---

## Usage

### Basic Invocation and Caching

Call the decorated function as usual. On the first call, the result is computed and stored in the cache. Subsequent calls with the same arguments load the result from the cache:

```python
result = expensive_function(4)  # Computes and stores the result
result = expensive_function(4)  # Loads the result from the cache
```

### Force Recalculation

Force a recalculation and overwrite the stored checkpoint:

```python
result = expensive_function.rerun(4)
```

### Call the Original Function

Use `fn` to directly call the original, undecorated function:

```python
result = expensive_function.fn(4)
```

This is especially useful **inside recursive functions** to avoid redundant caching of intermediate steps while still caching the final result.

### Retrieve Stored Checkpoints

Access cached results without recalculating:

```python
stored_result = expensive_function.get(4)
```

---

## Storage Backends

`checkpointer` works with both built-in and custom storage backends, so you can use what's provided or roll your own as needed.

### Built-In Backends

1. **PickleStorage**: Stores checkpoints on disk using Python's `pickle`.
2. **MemoryStorage**: Keeps checkpoints in memory for non-persistent, fast caching.

You can specify a storage backend using either its name (`"pickle"` or `"memory"`) or its corresponding class (`PickleStorage` or `MemoryStorage`) in the `format` parameter:

```python
from checkpointer import checkpoint, PickleStorage, MemoryStorage

@checkpoint(format="pickle")  # Short for format=PickleStorage
def disk_cached(x: int) -> int:
    return x ** 2

@checkpoint(format="memory")  # Short for format=MemoryStorage
def memory_cached(x: int) -> int:
    return x * 10
```

### Custom Storage Backends

Create a custom storage backend by inheriting from the `Storage` class and implementing its methods. Access configuration options through the `self.checkpointer` attribute, an instance of `Checkpointer`.

#### Example: Custom Storage Backend

```python
from checkpointer import checkpoint, Storage
from datetime import datetime

class CustomStorage(Storage):
    def exists(self, path) -> bool: ...  # Check if a checkpoint exists at the given path
    def checkpoint_date(self, path) -> datetime: ...  # Return the date the checkpoint was created
    def store(self, path, data): ...  # Save the checkpoint data
    def load(self, path): ...  # Return the checkpoint data
    def delete(self, path): ...  # Delete the checkpoint

@checkpoint(format=CustomStorage)
def custom_cached(x: int):
    return x ** 2
```

Using a custom backend lets you tailor storage to your application, whether it involves databases, cloud storage, or custom file formats.

---

## Configuration Options ⚙️

| Option          | Type                              | Default              | Description                                    |
|-----------------|-----------------------------------|----------------------|------------------------------------------------|
| `capture`       | `bool`                            | `False`              | Include captured variables in function hashes. |
| `format`        | `"pickle"`, `"memory"`, `Storage` | `"pickle"`           | Storage backend format.                        |
| `root_path`     | `Path`, `str`, or `None`          | ~/.cache/checkpoints | Root directory for storing checkpoints.        |
| `when`          | `bool`                            | `True`               | Enable or disable checkpointing.               |
| `verbosity`     | `0` or `1`                        | `1`                  | Logging verbosity.                             |
| `path`          | `Callable[..., str]`              | `None`               | Custom path for checkpoint storage.            |
| `should_expire` | `Callable[[datetime], bool]`      | `None`               | Custom expiration logic.                       |

---

## Full Example 🛠️

```python
import asyncio
from checkpointer import checkpoint

@checkpoint
def compute_square(n: int) -> int:
    print(f"Computing {n}^2...")
    return n ** 2

@checkpoint(format="memory")
async def async_compute_sum(a: int, b: int) -> int:
    await asyncio.sleep(1)
    return a + b

async def main():
    result1 = compute_square(5)
    print(result1)  # Outputs 25

    result2 = await async_compute_sum(3, 7)
    print(result2)  # Outputs 10

    result3 = await async_compute_sum.get(3, 7)
    print(result3)  # Outputs 10

asyncio.run(main())
```
