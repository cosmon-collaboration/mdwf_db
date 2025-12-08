"""Dynamic registry for job and input context builders.

Builders are discovered automatically from MDWFutils.jobs.* modules so new
builders are picked up without maintaining a static map.
"""

from __future__ import annotations

import inspect
import pkgutil
import re
from functools import lru_cache
from importlib import import_module
from typing import Callable, Dict, List, Tuple, Optional, Type

from .schema import ContextBuilder, _deduplicate_schema, WitGPUContextBuilder


# --------------------------------------------------------------------------- #
# Discovery helpers
# --------------------------------------------------------------------------- #

# Modules to skip during discovery (non-builder or helper modules)
_SKIP_MODULES = {
    "registry",
    "schema",
    "utils",
    "__init__",
    "hmc_helpers",
    "hmc_resubmit",
}

# Class-name overrides for type naming (handles consecutive capitals)
_TYPE_OVERRIDES = {
    "HMCGPUContextBuilder": "hmc_gpu",
    "HMCCPUContextBuilder": "hmc_cpu",
    "HMCXMLContextBuilder": "hmc_xml",
    "MresMQContextBuilder": "mres_mq",
}


def _class_to_type_name(cls: Type[ContextBuilder]) -> str:
    """Convert a ContextBuilder class name to a snake-case type name."""
    name = cls.__name__
    if name in _TYPE_OVERRIDES:
        return _TYPE_OVERRIDES[name]
    base = re.sub(r"ContextBuilder$", "", name)
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", base).lower()
    return snake


def _discover_builders() -> Tuple[Dict[str, Type[ContextBuilder]], Dict[str, Type[ContextBuilder]]]:
    """Discover job and input builders dynamically from MDWFutils.jobs modules."""
    import MDWFutils.jobs as jobs_pkg

    job_builders: Dict[str, Type[ContextBuilder]] = {}
    input_builders: Dict[str, Type[ContextBuilder]] = {}

    for _, module_name, ispkg in pkgutil.iter_modules(jobs_pkg.__path__):
        if ispkg or module_name in _SKIP_MODULES:
            continue
        module = import_module(f"{jobs_pkg.__name__}.{module_name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if not issubclass(obj, ContextBuilder):
                continue
            if obj in (ContextBuilder, WitGPUContextBuilder):
                continue  # skip bases

            type_name = _class_to_type_name(obj)

            # Heuristic: if job_params_schema has entries, treat as job builder; otherwise input.
            job_schema = getattr(obj, "job_params_schema", None)
            if job_schema:
                job_builders[type_name] = obj
            else:
                input_builders[type_name] = obj

    return job_builders, input_builders


@lru_cache(maxsize=None)
def _builder_maps():
    return _discover_builders()


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=None)
def get_job_builder(job_type: str):
    """Return an INSTANCE of the job builder, ready to call .build()."""
    job_builders, _ = _builder_maps()
    cls = job_builders.get(job_type)
    if cls is None:
        raise KeyError(f"Unknown job_type '{job_type}'")
    return cls()


@lru_cache(maxsize=None)
def get_input_builder(input_type: str):
    """Return an INSTANCE of the input builder, ready to call .build()."""
    _, input_builders = _builder_maps()
    cls = input_builders.get(input_type)
    if cls is None:
        raise KeyError(f"Unknown input_type '{input_type}'")
    return cls()


def get_job_schema(job_type: str) -> Tuple[Optional[List], Optional[List]]:
    """Get schemas from builder CLASS (not instance), deduplicated."""
    job_builders, _ = _builder_maps()
    cls = job_builders.get(job_type)
    if cls is None:
        return (None, None)
    job_schema = getattr(cls, "job_params_schema", None)
    input_schema = getattr(cls, "input_params_schema", None)
    return _deduplicate_schema(job_schema), _deduplicate_schema(input_schema)


def get_input_schema(input_type: str) -> Optional[List]:
    """Get input_params_schema from input builder CLASS (not instance), deduplicated."""
    _, input_builders = _builder_maps()
    cls = input_builders.get(input_type)
    if cls is None:
        return None
    schema = getattr(cls, "input_params_schema", None)
    return _deduplicate_schema(schema)


__all__ = [
    "get_job_builder",
    "get_input_builder",
    "get_job_schema",
    "get_input_schema",
]

