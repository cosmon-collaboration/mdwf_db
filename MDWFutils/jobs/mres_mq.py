"""Residual mass with modified heavy quark mass (mres_mq) context builder."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from MDWFutils.exceptions import ValidationError

from .mres import _convert_cli_params
from .schema import ContextParam
from .utils import (
    compute_kappa,
    get_ensemble_doc,
    get_physics_params,
    parse_ogeom,
    validate_geometry,
)

DEFAULT_WIT_ENV = "source /global/cfs/cdirs/m2986/cosmon/mdwf/software/scripts/env_gpu.sh"
DEFAULT_WIT_BIND = "/global/cfs/cdirs/m2986/cosmon/mdwf/ANALYSIS/WIT/bind.sh"
DEFAULT_WIT_EXEC = "/global/cfs/cdirs/m2986/cosmon/mdwf/software/install_gpu/wit/bin/Mres"
DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_OGEOM = "1,1,1,4"


class MresMQContextBuilder:
    """Residual mass with modified heavy quark mass context builder with declarative parameter schema."""
    
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
        ContextParam("mc", float, help="Charm quark mass override"),
    ]
    
    input_params_schema = [
        ContextParam("Configurations.first", int, required=True, help="First configuration"),
        ContextParam("Configurations.last", int, required=True, help="Last configuration"),
        ContextParam("Configurations.step", int, default=4, help="Configuration step"),
    ]
    
    def build(self, backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
        """Build the context payload for the mres_mq SLURM template."""
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

        mc_override = job_params.get("mc")
        if mc_override is not None:
            try:
                mc = float(mc_override)
            except ValueError as exc:
                raise ValidationError(f"mc override must be numeric, got {mc_override}") from exc

        if mc <= 0:
            raise ValidationError("mc must be positive for mres_mq jobs")

        kappa_c = compute_kappa(mc)

        work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
        workdir = work_root / "mres_mq"
        log_dir = workdir / "jlog"
        workdir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        (workdir / "slurm").mkdir(parents=True, exist_ok=True)

        ogeom = parse_ogeom(str(job_params.get("ogeom") or DEFAULT_OGEOM))
        lgeom = validate_geometry(L, T, ogeom)

        # WIT input will be written by BaseCommand using build_wit_context
        # We specify where via _input_output_dir
        wit_input_path = workdir / "DWF_mres_mq.in"

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
            "job_name": job_params.get("job_name") or f"mresmq_{ensemble_id}",
            "mail_user": job_params.get("mail_user") or "",
            "log_dir": str(log_dir),
            "separate_error_log": False,
            "ensemble_id": ensemble_id,
            "operation": "WIT_MRES_MQ",
            "config_start": config_start,
            "config_end": config_end,
            "config_inc": config_inc,
            "run_dir": str(work_root),
            "params": f"kappaC={kappa_c:.6f}",
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
            "_output_prefix": f"mres_mq_{config_start}_{config_end}",
            # Tell BaseCommand where to put the WIT input file
            "_input_output_dir": str(workdir),
            "_input_output_prefix": "DWF_mres_mq",
        }


# Backward compatibility: function wrapper
def build_mres_mq_context(backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
    """Legacy function wrapper for backward compatibility."""
    builder = MresMQContextBuilder()
    return builder.build(backend, ensemble_id, job_params, input_params)


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

