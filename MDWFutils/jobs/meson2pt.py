"""Meson two-point (meson2pt) job context builder."""

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
DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Meson"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_OGEOM = "1,1,1,4"


def build_meson2pt_context(
    backend, ensemble_id: int, job_params: Dict, input_params: Dict
) -> Dict:
    """Build the context dictionary for rendering meson2pt SLURM templates."""
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

    ml = _maybe_override(job_params.get("ml"), ml, "ml")
    ms = _maybe_override(job_params.get("ms"), ms, "ms")
    mc = _maybe_override(job_params.get("mc"), mc, "mc")

    kappa_l = compute_kappa(ml)
    kappa_s = compute_kappa(ms)
    kappa_c = compute_kappa(mc)

    work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
    workdir = work_root / "meson2pt"
    log_dir = workdir / "jlog"
    workdir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    (workdir / "slurm").mkdir(parents=True, exist_ok=True)

    ogeom = parse_ogeom(str(job_params.get("ogeom") or DEFAULT_OGEOM))
    lgeom = validate_geometry(L, T, ogeom)

    wit_input_path = workdir / "DWF_meson2pt.in"
    wit_overrides = _convert_cli_params(input_params)
    _apply_meson_defaults(wit_overrides, kappa_l, kappa_s, kappa_c)

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
        "job_name": job_params.get("job_name") or f"meson2pt_{ensemble_id}",
        "mail_user": job_params.get("mail_user") or "",
        "log_dir": str(log_dir),
        "separate_error_log": False,
        "ensemble_id": ensemble_id,
        "operation": "WIT_MESON2PT",
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
        "_output_prefix": f"meson2pt_{config_start}_{config_end}",
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

