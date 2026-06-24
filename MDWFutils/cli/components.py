"""Composable CLI components."""

from __future__ import annotations

from typing import Dict, List

from ..backends.base import DatabaseBackend
from ..exceptions import EnsembleNotFoundError
from ..jobs.registry import get_input_builder
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
    """Load, merge, and persist parameter dicts."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

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

    # ------------------------------------------------------------------
    # Ensemble defaults (per-param, per-command storage)
    # ------------------------------------------------------------------
    def load_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
    ) -> Dict[str, Dict[str, str]]:
        """Load per-param defaults, falling back to legacy string storage."""
        defaults = self.backend.get_ensemble_defaults(ensemble_id, command, variant)
        if defaults.get("input_params") or defaults.get("job_params"):
            return defaults

        # Try legacy nested defaults if backend supports it
        if hasattr(self.backend, "get_legacy_default_params"):
            try:
                legacy = self.backend.get_legacy_default_params(
                    ensemble_id, command, variant
                )
                input_raw = legacy.get("input_params", "")
                job_raw = legacy.get("job_params", "")
                if input_raw or job_raw:
                    return {
                        "input_params": self.parse(input_raw),
                        "job_params": self.parse(job_raw),
                    }
            except EnsembleNotFoundError:
                pass

        return {"input_params": {}, "job_params": {}}

    def save_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
        input_params: Dict[str, str],
        job_params: Dict[str, str],
    ) -> bool:
        """Save per-param defaults to the ensemble_defaults collection, merging with existing."""
        existing = self.load_ensemble_defaults(ensemble_id, command, variant)
        
        merged_input = self.merge(existing.get("input_params", {}), input_params)
        merged_job = self.merge(existing.get("job_params", {}), job_params)
        
        return self.backend.set_ensemble_defaults(
            ensemble_id, command, variant, merged_input, merged_job
        )

    def delete_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
    ) -> bool:
        """Delete defaults for a command/variant."""
        return self.backend.delete_ensemble_defaults(ensemble_id, command, variant)

    def list_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str = None,
    ) -> List[Dict]:
        """List all defaults for an ensemble."""
        return self.backend.list_ensemble_defaults(ensemble_id, command)


class BuildScriptGenerator:
    """Generate build shell scripts and sources using templates."""

    def __init__(self, backend: DatabaseBackend | None = None):
        self.backend = backend
        self.renderer = TemplateRenderer(TemplateLoader())

    def generate(
        self,
        type_name: str,
        ensemble_id: int,
        build_params: Dict,
        *,
        ensemble=None,
        command_line: str = "",
    ) -> str:
        from ..build.registry import get_build_builder

        builder = get_build_builder(type_name)
        template = getattr(builder, "template_name", None) or f"build/{type_name}.j2"
        context = builder.build(
            self.backend,
            ensemble_id,
            build_params,
            ensemble=ensemble,
            command_line=command_line,
        )
        return self.renderer.render(template, context), context


class ScriptGenerator:
    """Generate input files using templates."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend
        self.renderer = TemplateRenderer(TemplateLoader())

    def generate_input(
        self,
        ensemble_id: int,
        input_type: str,
        params: Dict,
        job_params: Dict | None = None,
    ) -> str:
        builder = get_input_builder(input_type)
        context = builder.build(self.backend, ensemble_id, job_params or {}, params)
        return self.renderer.render(f"input/{input_type}.j2", context)

