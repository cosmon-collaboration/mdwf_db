"""Smear job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import ContextBuilder, ContextParam, common_slurm_params, DEFAULT_CONDA_ENV, DEFAULT_GLU_EXEC, DEFAULT_CONFIG_PREFIX
DEFAULT_OUTPUT_PREFIX = "u_"
DEFAULT_NSIM = 8


class SmearContextBuilder(ContextBuilder):
    """Smear job context builder"""
    
    job_params_schema = [
        *common_slurm_params(),
        # Override constraint for CPU jobs
        ContextParam("constraint", str, default="cpu", help="Node constraint"),
        # Smear-specific params
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
        ContextParam("SMEARTYPE", str, default="STOUT", choices=["STOUT", "APE", "HYP"], help="Smearing algorithm"),
        ContextParam("SMITERS", int, default=8, help="Smearing iterations"),
        ContextParam("ALPHA1", float, help="Alpha1 parameter"),
        ContextParam("ALPHA2", float, help="Alpha2 parameter"),
        ContextParam("ALPHA3", float, help="Alpha3 parameter"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Build smear context. Schema params auto-merged."""
        try:
            L = int(physics["L"])
            T = int(physics["T"])
        except KeyError as exc:
            raise ValidationError("Ensemble is missing L/T lattice dimensions") from exc

        work_root = self._resolve_run_dir(ensemble, job_params)
        
        # Extract values needed for computations (schema ensures they exist)
        smear_type = input_params["SMEARTYPE"]
        smiters = input_params["SMITERS"]
        config_start = job_params["config_start"]
        config_end = job_params["config_end"]
        
        # Compute derived values
        smear_dir = work_root / f"cnfg_{smear_type}{smiters}"
        log_dir = smear_dir / "jlog"
        log_dir.mkdir(parents=True, exist_ok=True)
        (smear_dir / "slurm").mkdir(parents=True, exist_ok=True)
        
        prefix_for_files = _determine_output_prefix(smear_type, smiters, job_params)
        glu_input_path = smear_dir / "glu_smear.in"
        
        # Return ONLY computed/special values
        # (account, constraint, queue, etc. auto-merged from schema)
        return {
            # Computed paths
            "log_dir": str(log_dir),
            "separate_error_log": True,
            "signal": "B:TERM@60",
            "smear_dir": str(smear_dir),
            "config_dir": str(work_root / "cnfg"),
            "prefix_for_files": prefix_for_files,
            "glu_input_path": str(glu_input_path),
            "glu_exec_path": job_params.get("glu_path"),
            # Dynamic default with fallback
            "job_name": job_params.get("job_name") or f"smear_{ensemble_id}",
            # Don't set ntasks_per_node - old script didn't use --ntasks-per-node
            # (ranks parameter exists but wasn't used in SBATCH header)
            # DB tracking
            "ensemble_id": ensemble_id,
            "operation": "GLU_SMEAR",
            "run_dir": str(work_root),
            "params": f"smear_type={smear_type} smiters={smiters}",
            "smear_type": smear_type,
            "smiters": smiters,
            # Template control
            "_output_dir": str(smear_dir / "slurm"),
            "_output_prefix": f"smear_{config_start}_{config_end}",
            "_input_output_dir": str(smear_dir),
            "_input_output_prefix": "glu_smear",
        }


def _determine_output_prefix(smear_type: str, smiters: int, job_params: Dict) -> str:
    """Replicate historical file prefix logic."""
    prefix = f"{DEFAULT_OUTPUT_PREFIX}{smear_type}{smiters}"
    custom = job_params.get("output_prefix")
    try:
        if smear_type.lower() == "stout" and int(smiters) == 8:
            prefix = "ck"
        elif custom and custom != DEFAULT_OUTPUT_PREFIX:
            prefix = f"{custom}{smear_type}{smiters}"
    except Exception:
        pass
    return prefix