import io
import os
import sys
from typing import Literal

Color = Literal[
  "black", "grey", "red", "green", "yellow", "blue", "magenta",
  "cyan", "light_grey", "dark_grey", "light_red", "light_green",
  "light_yellow", "light_blue", "light_magenta", "light_cyan", "white",
]

COLOR_MAP: dict[Color, int] = {
  "black": 30,
  "grey": 30,
  "red": 31,
  "green": 32,
  "yellow": 33,
  "blue": 34,
  "magenta": 35,
  "cyan": 36,
  "light_grey": 37,
  "dark_grey": 90,
  "light_red": 91,
  "light_green": 92,
  "light_yellow": 93,
  "light_blue": 94,
  "light_magenta": 95,
  "light_cyan": 96,
  "white": 97,
}

def allow_color() -> bool:
  if "NO_COLOR" in os.environ or os.environ.get("TERM") == "dumb" or not hasattr(sys.stdout, "fileno"):
    return False
  try:
    return os.isatty(sys.stdout.fileno())
  except io.UnsupportedOperation:
    return sys.stdout.isatty()

def colored_(text: str, color: Color | None = None, on_color: Color | None = None) -> str:
  if color:
    text = f"\033[{COLOR_MAP[color]}m{text}"
  if on_color:
    text = f"\033[{COLOR_MAP[on_color] + 10}m{text}"
  return text + "\033[0m"

noop = lambda *args, **_: args[0]
colored = colored_ if allow_color() else noop

def print_checkpoint(should_log: bool, title: str, text: str, color: Color):
  if should_log:
    print(f"{colored(f" {title} ", "grey", color)} {colored(text, color)}")
