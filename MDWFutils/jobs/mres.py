"""Residual mass (mres) job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .utils import (
    get_ensemble_doc,
    get_physics_params,
    parse_ogeom,
    validate_geometry,
    compute_kappa,
)
from .wit import generate_wit_input

DEFAULT_WIT_ENV = "source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh"
DEFAULT_WIT_BIND = "/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_OGEOM = "1,1,1,4"


def build_mres_context(backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
    """Return the rendering context for the mres SLURM template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)

    try:
        L = int(physics["L"])
        T = int(physics["T"])
        ml = float(physics["ml"])
        ms = float(physics["ms"])
        mc = float(physics["mc"])
    except KeyError as exc:
        raise ValidationError("Ensemble is missing required physics parameters") from exc

    kappa_l = compute_kappa(ml)
    kappa_s = compute_kappa(ms)
    kappa_c = compute_kappa(mc)

    work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
    workdir = work_root / "mres"
    log_dir = workdir / "jlog"
    workdir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (workdir / "slurm").mkdir(parents=True, exist_ok=True)

    ogeom_str = job_params.get("ogeom") or DEFAULT_OGEOM
    ogeom = parse_ogeom(str(ogeom_str))
    lgeom = validate_geometry(L, T, ogeom)

    wit_input_path = workdir / "DWF_mres.in"
    wit_overrides = _convert_cli_params(input_params)
    ensemble_params = {
        **physics,
        "ml": ml,
        "ms": ms,
        "mc": mc,
    }
    generate_wit_input(
        str(wit_input_path),
        custom_params=wit_overrides,
        ensemble_params=ensemble_params,
        cli_format=True,
    )

    config_start = int(input_params["Configurations.first"])
    config_end = int(input_params["Configurations.last"])
    config_inc = int(input_params.get("Configurations.step", 4))

    return {
        "account": job_params.get("account", "m2986_g"),
        "constraint": job_params.get("constraint", "gpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "06:00:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "gpus": int(job_params.get("gpus", 4)),
        "gpu_bind": job_params.get("gpu_bind", "none"),
        "job_name": job_params.get("job_name") or f"mres_{ensemble_id}",
        "mail_user": job_params.get("mail_user") or "",
        "log_dir": str(log_dir),
        "separate_error_log": False,
        "ensemble_id": ensemble_id,
        "operation": "WIT_MRES",
        "config_start": config_start,
        "config_end": config_end,
        "config_inc": config_inc,
        "run_dir": str(work_root),
        "params": f"kappaL={kappa_l:.6f} kappaS={kappa_s:.6f} kappaC={kappa_c:.6f}",
        "workdir": str(workdir),
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "env_setup": DEFAULT_WIT_ENV,
        "bind_script": job_params.get("bind_script", DEFAULT_WIT_BIND),
        "wit_exec_path": job_params.get("wit_exec_path", DEFAULT_WIT_EXEC),
        "wit_input_path": str(wit_input_path),
        "ogeom": " ".join(str(x) for x in ogeom),
        "lgeom": " ".join(str(x) for x in lgeom),
        "ranks": int(job_params.get("ranks", 4)),
        "_output_dir": str(workdir / "slurm"),
        "_output_prefix": f"mres_{config_start}_{config_end}",
    }


def _convert_cli_params(input_params: Dict) -> Dict:
    """
    Convert dotted CLI parameters (e.g. 'Configurations.first=10') into the nested
    dictionary structure expected by generate_wit_input when cli_format=True.
    """
    nested: Dict[str, Dict[str, str]] = {}
    for key, value in input_params.items():
        if "." not in key:
            nested.setdefault(key, {})["value"] = str(value)
            continue
        section, field = key.split(".", 1)
        section_name = section.replace("_", " ")
        nested.setdefault(section_name, {})[field] = str(value)
    return nested

