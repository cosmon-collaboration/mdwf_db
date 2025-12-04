"""Meson two-point (meson2pt) job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import WitGPUContextBuilder, ContextParam, common_wit_gpu_params, DEFAULT_WIT_ENV
from .utils import compute_kappa

DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Meson"


class Meson2ptContextBuilder(WitGPUContextBuilder):
    """Meson two-point job context builder with declarative parameter schema."""
    
    job_params_schema = [
        *common_wit_gpu_params(),
        # Meson2pt-specific params
        ContextParam("wit_exec_path", str, default=DEFAULT_WIT_EXEC, help="WIT executable path"),
        ContextParam("ml", float, help="Light quark mass override"),
        ContextParam("ms", float, help="Strange quark mass override"),
        ContextParam("mc", float, help="Charm quark mass override"),
    ]
    
    input_params_schema = [
        ContextParam("Configurations.first", int, required=True, help="First configuration"),
        ContextParam("Configurations.last", int, required=True, help="Last configuration"),
        ContextParam("Configurations.step", int, default=4, help="Configuration step"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Build the context dictionary for rendering meson2pt SLURM templates."""
        try:
            ml = float(physics["ml"])
            ms = float(physics["ms"])
            mc = float(physics["mc"])
        except KeyError as exc:
            raise ValidationError("Ensemble is missing required physics parameters") from exc

        ml = _maybe_override(job_params.get("ml"), ml, "ml")
        ms = _maybe_override(job_params.get("ms"), ms, "ms")
        mc = _maybe_override(job_params.get("mc"), mc, "mc")

        kappa_l = compute_kappa(ml)
        kappa_s = compute_kappa(ms)
        kappa_c = compute_kappa(mc)

        # Use shared helper methods
        workdir, log_dir = self._setup_wit_workdir(ensemble, job_params, "meson2pt")
        L, T, ogeom, lgeom = self._parse_geometry(physics, job_params)

        wit_input_path = workdir / "DWF_meson2pt.in"

        # Extract values needed for computations
        config_start = input_params["Configurations.first"]
        config_end = input_params["Configurations.last"]

        # Return ONLY computed/special values
        return {
            "log_dir": str(log_dir),
            "separate_error_log": False,
            "job_name": job_params.get("job_name") or f"meson2pt_{ensemble_id}",
            "ensemble_id": ensemble_id,
            "operation": "WIT_MESON2PT",
            "run_dir": str(self._resolve_run_dir(ensemble, job_params)),
            "params": f"kappaL={kappa_l:.6f} kappaS={kappa_s:.6f} kappaC={kappa_c:.6f}",
            "workdir": str(workdir),
            "env_setup": DEFAULT_WIT_ENV,
            "wit_input_path": str(wit_input_path),
            "ogeom": " ".join(str(x) for x in ogeom),
            "lgeom": " ".join(str(x) for x in lgeom),
            "_output_dir": str(workdir / "slurm"),
            "_output_prefix": f"meson2pt_{config_start}_{config_end}",
            "_input_output_dir": str(workdir),
            "_input_output_prefix": "DWF_meson2pt",
        }


def _apply_meson_defaults(wit_params: Dict, kappa_l: float, kappa_s: float, kappa_c: float) -> None:
    """Seed propagator sections with kappas mirroring the legacy script."""
    for idx, kappa in enumerate((kappa_l, kappa_s, kappa_c)):
        section = f"Propagator {idx}"
        block = wit_params.setdefault(section, {})
        block.setdefault("kappa", str(kappa))


def _maybe_override(raw_value, current: float, label: str) -> float:
    """Validate optional mass overrides."""
    if raw_value is None:
        return current
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValidationError(f"{label} override must be numeric, got {raw_value}") from exc
    if value <= 0:
        raise ValidationError(f"{label} must be positive, got {value}")
    return value

