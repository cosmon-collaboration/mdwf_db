"""Composable CLI components."""

from __future__ import annotations

from typing import Dict

from ..backends.base import DatabaseBackend
from ..exceptions import EnsembleNotFoundError
from ..jobs.registry import get_input_builder, get_job_builder
from ..templates.loader import TemplateLoader
from ..templates.renderer import TemplateRenderer


class EnsembleResolver:
    """Resolve ensemble identifiers via backend."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

    def resolve(self, identifier):
        ensemble_id, ensemble = self.backend.resolve_ensemble_identifier(identifier)
        if not ensemble:
            raise EnsembleNotFoundError(identifier)
        return ensemble_id, ensemble


class ParameterManager:
    """Load, merge, and persist parameter strings."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

    def load_defaults(self, ensemble_id: int, job_type: str, variant: str) -> Dict[str, str]:
        return self.backend.get_default_params(ensemble_id, job_type, variant)

    @staticmethod
    def parse(param_string: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for token in (param_string or "").split():
            if "=" in token:
                key, value = token.split("=", 1)
                result[key] = value
        return result

    @staticmethod
    def merge(defaults: Dict[str, str], overrides: Dict[str, str]) -> Dict[str, str]:
        merged = defaults.copy()
        merged.update(overrides)
        return merged

    def save_defaults(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
        input_params: str,
        job_params: str,
    ) -> bool:
        return self.backend.set_default_params(
            ensemble_id,
            job_type,
            variant,
            input_params,
            job_params,
        )


class ScriptGenerator:
    """Generate input files and SLURM scripts using templates."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend
        self.renderer = TemplateRenderer(TemplateLoader())

    def generate_input(self, ensemble_id: int, input_type: str, params: Dict) -> str:
        builder = get_input_builder(input_type)
        context = builder(self.backend, ensemble_id, params)
        return self.renderer.render(f"input/{input_type}.j2", context)

    def generate_slurm(
        self,
        ensemble_id: int,
        job_type: str,
        job_params: Dict,
        input_params: Dict,
    ) -> str:
        builder = get_job_builder(job_type)
        context = builder(self.backend, ensemble_id, job_params, input_params)
        return self.renderer.render(f"slurm/{job_type}.j2", context)


