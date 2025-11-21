"""Registry for job and input context builders.

Builders are referenced lazily so that modules can be refactored incrementally
without creating circular imports or requiring builder functions to exist
before they are implemented.
"""

from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from typing import Callable, Dict


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


# Mapping of job_type/input_type to their builder import paths. Builders will be
# implemented progressively in their respective job modules.
JOB_BUILDERS: Dict[str, str] = {
    "smear": "MDWFutils.jobs.smear:build_smear_context",
    "wflow": "MDWFutils.jobs.wflow:build_wflow_context",
    "mres": "MDWFutils.jobs.mres:build_mres_context",
    "mres_mq": "MDWFutils.jobs.mres_mq:build_mres_mq_context",
    "meson2pt": "MDWFutils.jobs.meson2pt:build_meson2pt_context",
    "zv": "MDWFutils.jobs.zv:build_zv_context",
    "hmc_gpu": "MDWFutils.jobs.hmc:build_hmc_gpu_context",
    "hmc_cpu": "MDWFutils.jobs.hmc:build_hmc_cpu_context",
}

INPUT_BUILDERS: Dict[str, str] = {
    "hmc_xml": "MDWFutils.jobs.hmc:build_hmc_xml_context",
    "glu_input": "MDWFutils.jobs.glu:build_glu_context",
    "wit_input": "MDWFutils.jobs.wit:build_wit_context",
}

__all__ = [
    "get_job_builder",
    "get_input_builder",
    "JOB_BUILDERS",
    "INPUT_BUILDERS",
]

