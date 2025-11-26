# MDWFutils/cli/__init__.py

from .introspect import get_command_metadata
from .main import main

__all__ = ['main', 'get_command_metadata']