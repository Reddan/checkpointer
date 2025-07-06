import ast
import sys
from inspect import getsource
from textwrap import dedent
from typing import Callable
from .utils import drop_none, get_at

def get_decorator_path(node: ast.AST) -> tuple[str, ...]:
  if isinstance(node, ast.Call):
    return get_decorator_path(node.func)
  elif isinstance(node, ast.Attribute):
    return get_decorator_path(node.value) + (node.attr,)
  elif isinstance(node, ast.Name):
    return (node.id,)
  else:
    return ()

def is_lone_expression(node: ast.AST) -> bool:
  # Filter out docstrings
  return isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)

class CleanFunctionTransform(ast.NodeTransformer):
  def __init__(self, fn_globals: dict):
    self.is_root = True
    self.fn_globals = fn_globals

  def is_checkpointer(self, node: ast.AST) -> bool:
    from .checkpoint import Checkpointer
    decorator = get_at(self.fn_globals, *get_decorator_path(node))
    return isinstance(decorator, Checkpointer) or decorator is Checkpointer

  def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef):
    fn_type = type(node).__name__
    fn_name = None if self.is_root else node.name
    args_by_type = [
      node.args.posonlyargs + node.args.args,
      drop_none([node.args.vararg]),
      sorted(node.args.kwonlyargs, key=lambda x: x.arg),
      drop_none([node.args.kwarg]),
    ]
    arg_kind_names = ",".join(f"{i}:{arg.arg}" for i, args in enumerate(args_by_type) for arg in args)
    header = " ".join(drop_none((fn_type, fn_name, arg_kind_names or None)))

    self.is_root = False

    return ast.List([
      ast.Constant(header),
      ast.List([child for child in node.decorator_list if not self.is_checkpointer(child)], ast.Load()),
      ast.List([self.visit(child) for child in node.body if not is_lone_expression(child)], ast.Load()),
    ], ast.Load())

  def visit_AsyncFunctionDef(self, node):
    return self.visit_FunctionDef(node)

def get_fn_aststr(fn: Callable) -> str:
  try:
    source = getsource(fn)
  except OSError:
    return ""
  try:
    tree = ast.parse(dedent(source), mode="exec")
    tree = tree.body[0]
  except SyntaxError:
    # lambda functions can cause SyntaxError in ast.parse
    return source.strip()

  if fn.__name__ != "<lambda>":
    tree = CleanFunctionTransform(fn.__globals__).visit(tree)
  else:
    tree = ast.List([node for node in ast.walk(tree) if isinstance(node, ast.Lambda)], ast.Load())

  if sys.version_info >= (3, 13):
    return ast.dump(tree, annotate_fields=False, show_empty=True)
  else:
    return ast.dump(tree, annotate_fields=False)
