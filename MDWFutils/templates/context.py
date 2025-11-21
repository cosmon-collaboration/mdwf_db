"""Context builders for template rendering."""

from __future__ import annotations

from typing import Dict

from ..exceptions import EnsembleNotFoundError
from ..backends.base import DatabaseBackend


class ContextBuilder:
    """Builds rendering context payloads for templates."""

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

    def _get_ensemble(self, ensemble_id: int) -> Dict:
        ensemble = self.backend.get_ensemble(ensemble_id)
        if not ensemble:
            raise EnsembleNotFoundError(ensemble_id)
        return ensemble

    def build_slurm_context(self, ensemble_id: int, job_params: Dict) -> Dict:
        """Return context for SLURM templates."""
        ensemble = self._get_ensemble(ensemble_id)
        context = {
            "ensemble_id": ensemble_id,
            "ensemble_dir": ensemble["directory"],
            "L": ensemble["physics"].get("L"),
            "T": ensemble["physics"].get("T"),
            "db_connection": self.backend.connection_string,
        }
        context.update(job_params)
        return context

    def build_input_context(self, ensemble_id: int, input_params: Dict) -> Dict:
        """Return context for input file templates."""
        ensemble = self._get_ensemble(ensemble_id)
        physics = ensemble["physics"]
        context = {
            "L": physics.get("L"),
            "T": physics.get("T"),
            "Ls": physics.get("Ls"),
            "beta": physics.get("beta"),
            "b": physics.get("b"),
            "ml": physics.get("ml"),
            "ms": physics.get("ms"),
            "mc": physics.get("mc"),
        }
        context.update(input_params)
        return context


