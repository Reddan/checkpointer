import ast
import inspect
import sys
from types import ModuleType
from typing import Iterable, Type
from .utils import cwd, get_file, is_user_file

ImportTarget = tuple[str, str | None]

cache: dict[tuple[str, int], dict[str, ImportTarget]] = {}

def generate_import_mappings(module: ModuleType) -> Iterable[tuple[str, ImportTarget]]:
  mod_path = get_file(module)
  if not is_user_file(mod_path):
    return
  mod_parts = list(mod_path.with_suffix("").relative_to(cwd).parts)
  source = inspect.getsource(module)
  tree = ast.parse(source)
  for node in ast.walk(tree):
    if isinstance(node, ast.Import):
      for alias in node.names:
        yield (alias.asname or alias.name, (alias.name, None))
    elif isinstance(node, ast.ImportFrom):
      target_mod = node.module or ""
      if node.level > 0:
        target_mod_parts = target_mod.split(".") * bool(target_mod)
        target_mod_parts = mod_parts[:-node.level] + target_mod_parts
        target_mod = ".".join(target_mod_parts)
      for alias in node.names:
        yield (alias.asname or alias.name, (target_mod, alias.name))

def get_import_mappings(module: ModuleType) -> dict[str, ImportTarget]:
  cache_key = (module.__name__, id(module))
  if cached := cache.get(cache_key):
    return cached
  import_mappings = dict(generate_import_mappings(module))
  return cache.setdefault(cache_key, import_mappings)

def resolve_annotation(module: ModuleType, attr_name: str | None) -> Type | None:
  if not attr_name:
    return None
  if anno := module.__annotations__.get(attr_name):
    return anno
  if next_pair := get_import_mappings(module).get(attr_name):
    next_module_name, next_attr_name = next_pair
    if next_module := sys.modules.get(next_module_name):
      return resolve_annotation(next_module, next_attr_name)
