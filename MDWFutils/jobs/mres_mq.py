"""Residual mass with modified heavy quark mass (mres_mq) context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import WitGPUContextBuilder, ContextParam, common_wit_gpu_params, DEFAULT_WIT_ENV
from .utils import compute_kappa

DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"


class MresMQContextBuilder(WitGPUContextBuilder):
    """Residual mass with modified heavy quark mass context builder with declarative parameter schema."""
    
    job_params_schema = [
        *common_wit_gpu_params(),
        # Mres_mq-specific params
        ContextParam("wit_exec_path", str, default=DEFAULT_WIT_EXEC, help="WIT executable path"),
        ContextParam("mc", float, help="Charm quark mass override"),
    ]
    
    input_params_schema = [
        ContextParam("Configurations.first", int, required=True, help="First configuration"),
        ContextParam("Configurations.last", int, required=True, help="Last configuration"),
        ContextParam("Configurations.step", int, default=4, help="Configuration step"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Build the context payload for the mres_mq SLURM template."""
        try:
            mc = float(physics["mc"])
        except KeyError as exc:
            raise ValidationError("Ensemble is missing required physics parameters") from exc

        mc_override = job_params.get("mc")
        if mc_override is not None:
            try:
                mc = float(mc_override)
            except ValueError as exc:
                raise ValidationError(f"mc override must be numeric, got {mc_override}") from exc

        if mc <= 0:
            raise ValidationError("mc must be positive for mres_mq jobs")

        kappa_c = compute_kappa(mc)

        # Use shared helper methods
        workdir, log_dir = self._setup_wit_workdir(ensemble, job_params, "mres_mq")
        L, T, ogeom, lgeom = self._parse_geometry(physics, job_params)

        wit_input_path = workdir / "DWF_mres_mq.in"

        # Extract values needed for computations
        config_start = input_params["Configurations.first"]
        config_end = input_params["Configurations.last"]

        # Return ONLY computed/special values
        return {
            "log_dir": str(log_dir),
            "separate_error_log": False,
            "job_name": job_params.get("job_name") or f"mresmq_{ensemble_id}",
            "ensemble_id": ensemble_id,
            "operation": "WIT_MRES_MQ",
            "run_dir": str(self._resolve_run_dir(ensemble, job_params)),
            "params": f"kappaC={kappa_c:.6f}",
            "workdir": str(workdir),
            "env_setup": DEFAULT_WIT_ENV,
            "wit_input_path": str(wit_input_path),
            "ogeom": " ".join(str(x) for x in ogeom),
            "lgeom": " ".join(str(x) for x in lgeom),
            "_output_dir": str(workdir / "slurm"),
            "_output_prefix": f"mres_mq_{config_start}_{config_end}",
            "_input_output_dir": str(workdir),
            "_input_output_prefix": "DWF_mres_mq",
        }


def _apply_mq_defaults(wit_params: Dict, kappa_c: float) -> None:
    """Ensure Witness/Solver/Propagator sections match legacy defaults."""
    witness = wit_params.setdefault("Witness", {})
    witness.setdefault("no_prop", "1")
    witness.setdefault("no_solver", "1")

    solver = wit_params.setdefault("Solver 0", {})
    solver.setdefault("exact_deflation", "false")

    prop = wit_params.setdefault("Propagator 0", {})
    prop.setdefault("kappa", str(kappa_c))
    prop.setdefault("res", "5E-15")
    prop.setdefault("sloppy_res", "5E-15")

