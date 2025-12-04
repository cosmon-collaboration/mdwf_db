"""Composable CLI components."""

from __future__ import annotations

from typing import Dict, List

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

    def list_all_defaults(self, ensemble_id: int, job_type: str = None) -> List[Dict]:
        """List all saved defaults, optionally filtered by job_type."""
        if hasattr(self.backend, 'list_default_params'):
            return self.backend.list_default_params(ensemble_id, job_type)
        # Fallback for backends that don't support this
        ensemble = self.backend.get_ensemble(ensemble_id)
        if not ensemble:
            return []
        defaults = ensemble.get("default_params", {})
        result = []
        for jt, variants in defaults.items():
            if job_type and jt != job_type:
                continue
            for variant_name, variant_data in variants.items():
                result.append({
                    "job_type": jt,
                    "variant": variant_name,
                    "input_params": variant_data.get("input_params", ""),
                    "job_params": variant_data.get("job_params", ""),
                })
        return result

    def delete_defaults(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        """Delete a specific variant."""
        return self.backend.delete_default_params(ensemble_id, job_type, variant)


class ScriptGenerator:
    """Generate input files and SLURM scripts using templates."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend
        self.renderer = TemplateRenderer(TemplateLoader())

    def generate_input(self, ensemble_id: int, input_type: str, params: Dict) -> str:
        builder = get_input_builder(input_type)
        context = builder.build(self.backend, ensemble_id, input_params=params)
        return self.renderer.render(f"input/{input_type}.j2", context)

    def generate_slurm(
        self,
        ensemble_id: int,
        job_type: str,
        job_params: Dict,
        input_params: Dict,
    ) -> str:
        builder = get_job_builder(job_type)
        context = builder.build(self.backend, ensemble_id, job_params, input_params)
        return self.renderer.render(f"slurm/{job_type}.j2", context)


