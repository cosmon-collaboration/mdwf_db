"""Registry for job and input context builders.

Builders are referenced lazily so that modules can be refactored incrementally
without creating circular imports or requiring builder functions to exist
before they are implemented.
"""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from typing import Callable, Dict, List, Tuple, Optional


def _load_builder(path: str) -> Callable:
    """Lazily import and return a builder function specified as module:attr."""
    module_path, attr = path.split(":")
    module = import_module(module_path, package=__package__)
    try:
        return getattr(module, attr)
    except AttributeError as exc:  # pragma: no cover - defensive
        raise ImportError(f"Builder '{attr}' not found in '{module_path}'") from exc


@lru_cache(maxsize=None)
def get_job_builder(job_type: str) -> Callable:
    """Return the context builder callable for a SLURM job type."""
    target = JOB_BUILDERS.get(job_type)
    if target is None:
        raise KeyError(f"Unknown job_type '{job_type}'")
    return _load_builder(target)


@lru_cache(maxsize=None)
def get_input_builder(input_type: str) -> Callable:
    """Return the context builder callable for an input file type."""
    target = INPUT_BUILDERS.get(input_type)
    if target is None:
        raise KeyError(f"Unknown input_type '{input_type}'")
    return _load_builder(target)


def get_job_schema(job_type: str) -> Tuple[Optional[List], Optional[List]]:
    """Extract job_params_schema and input_params_schema from a job builder.
    
    Returns:
        Tuple of (job_params_schema, input_params_schema) or (None, None) if builder
        doesn't have schema attributes (backward compatibility with function builders).
    """
    try:
        builder = get_job_builder(job_type)
        # Check if builder has schema attributes (class-based builder)
        job_schema = getattr(builder, 'job_params_schema', None)
        input_schema = getattr(builder, 'input_params_schema', None)
        return (job_schema, input_schema)
    except (KeyError, ImportError):
        return (None, None)


def get_input_schema(input_type: str) -> Optional[List]:
    """Extract input_params_schema from an input builder.
    
    Returns:
        input_params_schema or None if builder doesn't have schema attribute.
    """
    try:
        builder = get_input_builder(input_type)
        return getattr(builder, 'input_params_schema', None)
    except (KeyError, ImportError):
        return None


# Mapping of job_type/input_type to their builder import paths. Builders will be
# implemented progressively in their respective job modules.
JOB_BUILDERS: Dict[str, str] = {
    "smear": "MDWFutils.jobs.smear:SmearContextBuilder",
    "wflow": "MDWFutils.jobs.wflow:WflowContextBuilder",
    "mres": "MDWFutils.jobs.mres:MresContextBuilder",
    "mres_mq": "MDWFutils.jobs.mres_mq:MresMQContextBuilder",
    "meson2pt": "MDWFutils.jobs.meson2pt:Meson2ptContextBuilder",
    "zv": "MDWFutils.jobs.zv:ZvContextBuilder",
    "hmc_gpu": "MDWFutils.jobs.hmc:HMCGPUContextBuilder",
    "hmc_cpu": "MDWFutils.jobs.hmc:HMCCPUContextBuilder",
}

INPUT_BUILDERS: Dict[str, str] = {
    "hmc_xml": "MDWFutils.jobs.hmc:build_hmc_xml_context",
    "glu_input": "MDWFutils.jobs.glu:build_glu_context",
    "wit_input": "MDWFutils.jobs.wit:build_wit_context",
}

__all__ = [
    "get_job_builder",
    "get_input_builder",
    "get_job_schema",
    "get_input_schema",
    "JOB_BUILDERS",
    "INPUT_BUILDERS",
]

