"""Zv measurement job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .mres import _convert_cli_params
from .utils import (
    compute_kappa,
    get_ensemble_doc,
    get_physics_params,
    parse_ogeom,
    validate_geometry,
)
from .wit import generate_wit_input

DEFAULT_WIT_ENV = "source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh"
DEFAULT_WIT_BIND = "/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/FDiagonal_3pt"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_OGEOM = "1,1,1,4"


def build_zv_context(
    backend, ensemble_id: int, job_params: Dict, input_params: Dict
) -> Dict:
    """Build the context payload for the Zv SLURM template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)

    try:
        L = int(physics["L"])
        T = int(physics["T"])
        ml = float(physics["ml"])
    except KeyError as exc:
        raise ValidationError("Ensemble is missing required physics parameters") from exc

    if ml <= 0:
        raise ValidationError("ml must be positive for Zv jobs")

    kappa_l = compute_kappa(ml)

    work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
    workdir = work_root / "Zv"
    log_dir = workdir / "jlog"
    workdir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (workdir / "slurm").mkdir(parents=True, exist_ok=True)

    ogeom = parse_ogeom(str(job_params.get("ogeom") or DEFAULT_OGEOM))
    lgeom = validate_geometry(L, T, ogeom)

    wit_input_path = workdir / "DWF_Zv.in"
    wit_overrides = _convert_cli_params(input_params)
    _apply_zv_defaults(wit_overrides, kappa_l)
    generate_wit_input(
        str(wit_input_path),
        custom_params=wit_overrides,
        ensemble_params=physics,
        cli_format=True,
        prune_prop_solvers=(1, 1),
    )

    config_start = int(input_params["Configurations.first"])
    config_end = int(input_params["Configurations.last"])
    config_inc = int(input_params.get("Configurations.step", 4))

    return {
        "account": job_params.get("account", "m2986_g"),
        "constraint": job_params.get("constraint", "gpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "00:10:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "gpus": int(job_params.get("gpus", 4)),
        "gpu_bind": job_params.get("gpu_bind", "none"),
        "job_name": job_params.get("job_name") or f"zv_{ensemble_id}",
        "mail_user": job_params.get("mail_user") or "",
        "log_dir": str(log_dir),
        "separate_error_log": False,
        "ensemble_id": ensemble_id,
        "db_file": getattr(backend, "connection_string", ""),
        "operation": "WIT_Zv",
        "config_start": config_start,
        "config_end": config_end,
        "config_inc": config_inc,
        "run_dir": str(work_root),
        "params": f"kappaL={kappa_l:.6f}",
        "workdir": str(workdir),
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "env_setup": DEFAULT_WIT_ENV,
        "bind_script": job_params.get("bind_script", DEFAULT_WIT_BIND),
        "wit_exec_path": job_params.get("wit_exec_path", DEFAULT_WIT_EXEC),
        "wit_input_path": str(wit_input_path),
        "ogeom": " ".join(str(x) for x in ogeom),
        "lgeom": " ".join(str(x) for x in lgeom),
        "ranks": int(job_params.get("ranks", 4)),
    }


def _apply_zv_defaults(wit_params: Dict, kappa_l: float) -> None:
    """Ensure Zv-specific Witness/Solver/Propagator defaults are seeded."""
    witness = wit_params.setdefault("Witness", {})
    witness.setdefault("no_prop", "1")
    witness.setdefault("no_solver", "1")

    prop = wit_params.setdefault("Propagator 0", {})
    prop.setdefault("kappa", str(kappa_l))

