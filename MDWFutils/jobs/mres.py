"""Residual mass (mres) job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import WitGPUContextBuilder, ContextParam, common_wit_gpu_params, DEFAULT_WIT_ENV
from .utils import compute_kappa

DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"


class MresContextBuilder(WitGPUContextBuilder):
    """Residual mass job context builder with declarative parameter schema."""
    
    job_params_schema = [
        *common_wit_gpu_params(),
        # Mres-specific params
        ContextParam("wit_exec_path", str, default=DEFAULT_WIT_EXEC, help="WIT executable path"),
    ]
    
    input_params_schema = [
        ContextParam("Configurations.first", int, required=True, help="First configuration"),
        ContextParam("Configurations.last", int, required=True, help="Last configuration"),
        ContextParam("Configurations.step", int, default=4, help="Configuration step"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Return the rendering context for the mres SLURM template."""
        try:
            ml = float(physics["ml"])
            ms = float(physics["ms"])
            mc = float(physics["mc"])
        except KeyError as exc:
            raise ValidationError("Ensemble is missing required physics parameters") from exc

        kappa_l = compute_kappa(ml)
        kappa_s = compute_kappa(ms)
        kappa_c = compute_kappa(mc)

        # Use shared helper methods
        workdir, log_dir = self._setup_wit_workdir(ensemble, job_params, "mres")
        L, T, ogeom, lgeom = self._parse_geometry(physics, job_params)

        wit_input_path = workdir / "DWF_mres.in"

        # Extract values needed for computations
        config_start = input_params["Configurations.first"]
        config_end = input_params["Configurations.last"]

        # Return ONLY computed/special values
        return {
            "log_dir": str(log_dir),
            "separate_error_log": False,
            "job_name": job_params.get("job_name") or f"mres_{ensemble_id}",
            "ensemble_id": ensemble_id,
            "operation": "WIT_MRES",
            "run_dir": str(self._resolve_run_dir(ensemble, job_params)),
            "params": f"kappaL={kappa_l:.6f} kappaS={kappa_s:.6f} kappaC={kappa_c:.6f}",
            "workdir": str(workdir),
            "env_setup": DEFAULT_WIT_ENV,
            "wit_input_path": str(wit_input_path),
            "ogeom": " ".join(str(x) for x in ogeom),
            "lgeom": " ".join(str(x) for x in lgeom),
            "_output_dir": str(workdir / "slurm"),
            "_output_prefix": f"mres_{config_start}_{config_end}",
            "_input_output_dir": str(workdir),
            "_input_output_prefix": "DWF_mres",
        }
