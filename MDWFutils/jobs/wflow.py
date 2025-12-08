"""Wilson flow job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import ContextBuilder, ContextParam, common_slurm_params, DEFAULT_CONDA_ENV, DEFAULT_GLU_EXEC, DEFAULT_CONFIG_PREFIX
DEFAULT_OUTPUT_PREFIX = "t0"
DEFAULT_SMEAR_TYPE = "ADAPTWFLOW_STOUT"
DEFAULT_SMITERS = 250
DEFAULT_NSIM = 4


class WflowContextBuilder(ContextBuilder):
    """Wilson flow job context builder with declarative parameter schema."""
    
    job_params_schema = [
        *common_slurm_params(),
        # Override constraint for CPU jobs
        ContextParam("constraint", str, default="cpu", help="Node constraint"),
        # Wflow-specific params
        ContextParam("config_start", int, required=True, help="First configuration"),
        ContextParam("config_end", int, required=True, help="Last configuration"),
        ContextParam("config_inc", int, default=4, help="Configuration increment"),
        ContextParam("run_dir", str, help="Working directory (defaults to ensemble directory)"),
        ContextParam("conda_env", str, default=DEFAULT_CONDA_ENV, help="Conda environment path"),
        ContextParam("config_prefix", str, default=DEFAULT_CONFIG_PREFIX, help="Configuration file prefix"),
        ContextParam("output_prefix", str, default=DEFAULT_OUTPUT_PREFIX, help="Output file prefix"),
        ContextParam("glu_path", str, default=DEFAULT_GLU_EXEC, help="GLU executable path"),
        ContextParam("nsim", int, default=DEFAULT_NSIM, help="Number of simultaneous configurations"),
    ]
    
    input_params_schema = [
        ContextParam("SMEARTYPE", str, default=DEFAULT_SMEAR_TYPE, help="Smearing algorithm"),
        ContextParam("SMITERS", int, default=DEFAULT_SMITERS, help="Smearing iterations"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Create context for the Wilson flow SLURM template."""
        try:
            L = int(physics["L"])
            T = int(physics["T"])
        except KeyError as exc:
            raise ValidationError("Ensemble is missing L/T lattice dimensions") from exc

        work_root = self._resolve_run_dir(ensemble, job_params)
        wflow_dir = work_root / "t0"
        log_dir = wflow_dir / "jlog"
        log_dir.mkdir(parents=True, exist_ok=True)
        (wflow_dir / "slurm").mkdir(parents=True, exist_ok=True)

        # Extract values needed for computations
        smear_type = input_params["SMEARTYPE"]
        smiters = input_params["SMITERS"]
        config_start = job_params["config_start"]
        config_end = job_params["config_end"]
        config_inc = job_params.get("config_inc", 4)
        
        glu_input_path = wflow_dir / "glu_smear.in"

        # Return ONLY computed/special values
        return {
            "log_dir": str(log_dir),
            "separate_error_log": True,
            "signal": "B:TERM@60",
            "job_name": job_params.get("job_name") or f"wflow_{ensemble_id}",
            "ensemble_id": ensemble_id,
            "operation": "GLU_WFLOW",
            "run_dir": str(work_root),
            "params": f"smear_type={smear_type} smiters={smiters}",
            "wflow_dir": str(wflow_dir),
            "config_dir": str(work_root / "cnfg"),
            "glu_input_path": str(glu_input_path),
            "glu_exec_path": job_params.get("glu_path"),
            "smear_type": smear_type,
            "smiters": smiters,
            "ntasks_per_node": job_params["ranks"],
            "config_start": config_start,
            "config_end": config_end,
            "config_inc": config_inc,
            "output_prefix": job_params.get("output_prefix", DEFAULT_OUTPUT_PREFIX),
            "_output_dir": str(wflow_dir / "slurm"),
            "_output_prefix": f"wflow_{config_start}_{config_end}",
            "_input_output_dir": str(wflow_dir),
            "_input_output_prefix": "glu_smear",
        }



