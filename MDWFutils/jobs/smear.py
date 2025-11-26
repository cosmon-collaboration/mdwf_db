"""Smear job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .glu import generate_glu_input
from .utils import get_ensemble_doc, get_physics_params

DEFAULT_GLU_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_CONFIG_PREFIX = "ckpoint_EODWF_lat."
DEFAULT_OUTPUT_PREFIX = "u_"
DEFAULT_NSIM = 8


def build_smear_context(
    backend, ensemble_id: int, job_params: Dict, input_params: Dict
) -> Dict:
    """
    Build the context dictionary required to render the smear SLURM template.
    """

    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)

    try:
        L = int(physics["L"])
        T = int(physics["T"])
    except KeyError as exc:
        raise ValidationError("Ensemble is missing L/T lattice dimensions") from exc

    ensemble_dir = Path(ensemble["directory"]).resolve()
    work_root = Path(job_params.get("run_dir") or ensemble_dir).resolve()

    smear_type = str(input_params.get("SMEARTYPE", "STOUT"))
    smiters = int(input_params.get("SMITERS", 8))
    alpha_values = [
        input_params.get("ALPHA1"),
        input_params.get("ALPHA2"),
        input_params.get("ALPHA3"),
    ]

    smear_dir = work_root / f"cnfg_{smear_type}{smiters}"
    log_dir = smear_dir / "jlog"
    log_dir.mkdir(parents=True, exist_ok=True)
    (smear_dir / "slurm").mkdir(parents=True, exist_ok=True)

    glu_input_path = smear_dir / "glu_smear.in"
    _write_glu_input(glu_input_path, L, T, job_params, smear_type, smiters, alpha_values)

    prefix_for_files = _determine_output_prefix(smear_type, smiters, job_params)

    config_start = int(job_params["config_start"])
    config_end = int(job_params["config_end"])
    config_inc = int(job_params.get("config_inc", 4))
    cpus_per_task = int(job_params.get("cpus_per_task", 256))
    nsim = int(job_params.get("nsim", DEFAULT_NSIM))

    context = {
        # SBATCH header
        "account": job_params.get("account", "m2986"),
        "constraint": job_params.get("constraint", "cpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "01:00:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "ntasks_per_node": int(job_params.get("ranks", 1)),
        "cpus_per_task": cpus_per_task,
        "job_name": job_params.get("job_name") or f"smear_{ensemble_id}",
        "mail_user": job_params.get("mail_user") or "",
        "log_dir": str(log_dir),
        "separate_error_log": True,
        "signal": "B:TERM@60",
        "mail_type": job_params.get("mail_type", "ALL"),
        # DB tracking
        "ensemble_id": ensemble_id,
        "operation": "GLU_SMEAR",
        "config_start": config_start,
        "config_end": config_end,
        "config_inc": config_inc,
        "run_dir": str(work_root),
        "params": f"smear_type={smear_type} smiters={smiters}",
        # Job-specific context
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "smear_dir": str(smear_dir),
        "config_dir": str(work_root / "cnfg"),
        "config_prefix": job_params.get("config_prefix", DEFAULT_CONFIG_PREFIX),
        "prefix_for_files": prefix_for_files,
        "glu_exec_path": job_params.get("glu_path", DEFAULT_GLU_EXEC),
        "glu_input_path": str(glu_input_path),
        "nsim": nsim,
        "_output_dir": str(smear_dir / "slurm"),
        "_output_prefix": f"smear_{config_start}_{config_end}",
    }

    return context


def _write_glu_input(
    target_path: Path,
    L: int,
    T: int,
    job_params: Dict,
    smear_type: str,
    smiters: int,
    alpha_values,
) -> None:
    """Generate the GLU input file with the appropriate overrides."""
    overrides = {
        "CONFNO": str(job_params["config_start"]),
        "DIM_0": str(L),
        "DIM_1": str(L),
        "DIM_2": str(L),
        "DIM_3": str(T),
        "SMEARTYPE": smear_type,
        "SMITERS": str(smiters),
    }

    alphas = ["ALPHA1", "ALPHA2", "ALPHA3"]
    for key, value in zip(alphas, alpha_values):
        if value is not None:
            overrides[key] = str(value)

    generate_glu_input(str(target_path), overrides)


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