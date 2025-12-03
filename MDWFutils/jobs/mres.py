"""Residual mass (mres) job context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .schema import ContextParam
from .utils import (
    get_ensemble_doc,
    get_physics_params,
    parse_ogeom,
    validate_geometry,
    compute_kappa,
)

DEFAULT_WIT_ENV = "source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh"
DEFAULT_WIT_BIND = "/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_OGEOM = "1,1,1,4"


class MresContextBuilder:
    """Residual mass job context builder with declarative parameter schema."""
    
    job_params_schema = [
        ContextParam("account", str, default="m2986_g", help="SLURM account"),
        ContextParam("constraint", str, default="gpu", help="Node constraint"),
        ContextParam("queue", str, default="regular", help="SLURM queue/partition"),
        ContextParam("time_limit", str, default="06:00:00", help="SLURM time limit"),
        ContextParam("nodes", int, default=1, help="Number of nodes"),
        ContextParam("gpus", int, default=4, help="GPUs per node"),
        ContextParam("gpu_bind", str, default="none", help="GPU binding policy"),
        ContextParam("ranks", int, default=4, help="MPI ranks"),
        ContextParam("run_dir", str, help="Working directory (defaults to ensemble directory)"),
        ContextParam("conda_env", str, default=DEFAULT_CONDA_ENV, help="Conda environment path"),
        ContextParam("bind_script", str, default=DEFAULT_WIT_BIND, help="CPU binding script"),
        ContextParam("wit_exec_path", str, default=DEFAULT_WIT_EXEC, help="WIT executable path"),
        ContextParam("ogeom", str, default=DEFAULT_OGEOM, help="Geometry override"),
    ]
    
    input_params_schema = [
        ContextParam("Configurations.first", int, required=True, help="First configuration"),
        ContextParam("Configurations.last", int, required=True, help="Last configuration"),
        ContextParam("Configurations.step", int, default=4, help="Configuration step"),
    ]
    
    def build(self, backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
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

        # WIT input will be written by BaseCommand using build_wit_context
        # We specify where via _input_output_dir
        wit_input_path = workdir / "DWF_mres.in"

        # Input params already have defaults applied from schema
        config_start = input_params["Configurations.first"]
        config_end = input_params["Configurations.last"]
        config_inc = input_params.get("Configurations.step", 4)

        return {
            "account": job_params.get("account", "m2986_g"),
            "constraint": job_params.get("constraint", "gpu"),
            "queue": job_params.get("queue", "regular"),
            "time_limit": job_params.get("time_limit", "06:00:00"),
            "nodes": job_params.get("nodes", 1),
            "gpus": job_params.get("gpus", 4),
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
            "ranks": job_params.get("ranks", 4),
            "_output_dir": str(workdir / "slurm"),
            "_output_prefix": f"mres_{config_start}_{config_end}",
            # Tell BaseCommand where to put the WIT input file
            "_input_output_dir": str(workdir),
            "_input_output_prefix": "DWF_mres",
        }


# Backward compatibility: function wrapper
def build_mres_context(backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
    """Legacy function wrapper for backward compatibility."""
    builder = MresContextBuilder()
    return builder.build(backend, ensemble_id, job_params, input_params)


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
