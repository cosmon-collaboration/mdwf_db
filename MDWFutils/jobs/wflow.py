"""Wilson flow job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .glu import generate_glu_input
from .utils import get_ensemble_doc, get_physics_params

DEFAULT_GLU_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install/GLU_ICC/bin/GLU"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_CONFIG_PREFIX = "ckpoint_EODWF_lat."
DEFAULT_OUTPUT_PREFIX = "t0"
DEFAULT_SMEAR_TYPE = "ADAPTWFLOW_STOUT"
DEFAULT_SMITERS = 250
DEFAULT_NSIM = 4


def build_wflow_context(
    backend, ensemble_id: int, job_params: Dict, input_params: Dict
) -> Dict:
    """Create context for the Wilson flow SLURM template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)

    try:
        L = int(physics["L"])
        T = int(physics["T"])
    except KeyError as exc:
        raise ValidationError("Ensemble is missing L/T lattice dimensions") from exc

    ensemble_dir = Path(ensemble["directory"]).resolve()
    work_root = Path(job_params.get("run_dir") or ensemble_dir).resolve()
    wflow_dir = work_root / "t0"
    log_dir = wflow_dir / "jlog"
    log_dir.mkdir(parents=True, exist_ok=True)
    (wflow_dir / "slurm").mkdir(parents=True, exist_ok=True)

    smear_type = str(input_params.get("SMEARTYPE", DEFAULT_SMEAR_TYPE))
    smiters = int(input_params.get("SMITERS", DEFAULT_SMITERS))

    glu_input_path = wflow_dir / "glu_smear.in"
    _write_glu_input(glu_input_path, L, T, job_params, smear_type, smiters, input_params)

    config_start = int(job_params["config_start"])
    config_end = int(job_params["config_end"])
    config_inc = int(job_params.get("config_inc", 4))
    cpus_per_task = int(job_params.get("cpus_per_task", 256))
    nsim = int(job_params.get("nsim", DEFAULT_NSIM))

    return {
        "account": job_params.get("account", "m2986"),
        "constraint": job_params.get("constraint", "cpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "01:00:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "ntasks_per_node": int(job_params.get("ranks", 1)),
        "cpus_per_task": cpus_per_task,
        "job_name": job_params.get("job_name") or f"wflow_{ensemble_id}",
        "mail_user": job_params.get("mail_user") or "",
        "log_dir": str(log_dir),
        "separate_error_log": True,
        "signal": "B:TERM@60",
        "mail_type": job_params.get("mail_type", "ALL"),
        "ensemble_id": ensemble_id,
        "db_file": getattr(backend, "connection_string", ""),
        "operation": "GLU_WFLOW",
        "config_start": config_start,
        "config_end": config_end,
        "config_inc": config_inc,
        "run_dir": str(work_root),
        "params": f"smear_type={smear_type} smiters={smiters}",
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "wflow_dir": str(wflow_dir),
        "config_dir": str(work_root / "cnfg"),
        "config_prefix": job_params.get("config_prefix", DEFAULT_CONFIG_PREFIX),
        "output_prefix": job_params.get("output_prefix", DEFAULT_OUTPUT_PREFIX),
        "glu_exec_path": job_params.get("glu_path", DEFAULT_GLU_EXEC),
        "glu_input_path": str(glu_input_path),
        "nsim": nsim,
    }


def _write_glu_input(
    target_path: Path,
    L: int,
    T: int,
    job_params: Dict,
    smear_type: str,
    smiters: int,
    input_params: Dict,
) -> None:
    """Generate the GLU input for Wilson flow."""
    overrides = {
        "CONFNO": str(job_params["config_start"]),
        "DIM_0": str(L),
        "DIM_1": str(L),
        "DIM_2": str(L),
        "DIM_3": str(T),
        "SMEARTYPE": smear_type,
        "SMITERS": str(smiters),
    }

    for key in ("ALPHA1", "ALPHA2", "ALPHA3"):
        if key in input_params:
            overrides[key] = str(input_params[key])

    generate_glu_input(str(target_path), overrides)