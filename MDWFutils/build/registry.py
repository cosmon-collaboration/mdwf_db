"""Dynamic registry for build context builders."""

from __future__ import annotations

import inspect
import pkgutil
from functools import lru_cache
from importlib import import_module
from typing import Dict, Type

from .schema import BuildContextBuilder, _deduplicate_schema


_SKIP_MODULES = {"registry", "schema", "__init__", "grid_catalog", "validate_grid_build", "action", "site", "operations", "params"}


def _discover_builders() -> Dict[str, Type[BuildContextBuilder]]:
    import MDWFutils.build as build_pkg

    builders: Dict[str, Type[BuildContextBuilder]] = {}
    builders_path = build_pkg.__path__[0] + "/builders"
    for _, module_name, ispkg in pkgutil.iter_modules([builders_path]):
        if ispkg or module_name.startswith("_"):
            continue
        module = import_module(f"{build_pkg.__name__}.builders.{module_name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if not issubclass(obj, BuildContextBuilder) or obj is BuildContextBuilder:
                continue
            type_name = getattr(obj, "type_name", None)
            if type_name:
                builders[type_name] = obj
    return builders


@lru_cache(maxsize=None)
def _builder_map() -> Dict[str, Type[BuildContextBuilder]]:
    return _discover_builders()


def get_build_builder(type_name: str) -> BuildContextBuilder:
    cls = _builder_map().get(type_name)
    if cls is None:
        raise KeyError(f"Unknown build type '{type_name}'")
    return cls()


def get_build_schema(type_name: str):
    cls = _builder_map().get(type_name)
    if cls is None:
        return None
    return _deduplicate_schema(getattr(cls, "build_params_schema", None))
