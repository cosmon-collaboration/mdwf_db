"""Context builders for HMC scripts and inputs."""

from __future__ import annotations

import random
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional
from xml.dom import minidom

from MDWFutils.exceptions import ValidationError

from .utils import (
    compute_kappa,
    ensure_keys,
    get_ensemble_doc,
    get_physics_params,
    require_all,
)

DEFAULT_CONDA_ENV = "/global/cfs/cdirs/m2986/cosmon/mdwf/scripts/cosmon_mdwf"
DEFAULT_LOGFILE = "/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"
DEFAULT_GPU_MPI = "4.4.4.8"
DEFAULT_CPU_MPI = "4.4.4.8"
DEFAULT_CPU_CACHEBLOCKING = "2.2.2.2"


# --------------------------------------------------------------------------- #
# HMC GPU / CPU context builders
# --------------------------------------------------------------------------- #

def build_hmc_gpu_context(backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
    """Return context for the GPU SLURM template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)
    ensure_keys(physics, ["L", "T", "beta", "b", "Ls", "ml", "ms", "mc"])

    paths = ensemble.get("paths", {})
    exec_path = job_params.get("exec_path") or paths.get("hmc_exec_path")
    if not exec_path:
        raise ValidationError("exec_path is required (set via CLI or ensemble paths.hmc_exec_path)")

    bind_script = (
        job_params.get("bind_script")
        or paths.get("hmc_bind_script_gpu")
        or paths.get("hmc_bind_script")
    )
    if not bind_script:
        raise ValidationError(
            "bind_script is required (set via CLI or ensemble paths.hmc_bind_script_gpu)"
        )

    required = require_all(job_params, {
        "n_trajec": "Number of trajectories per job",
        "trajL": "Trajectory length",
        "lvl_sizes": "Level sizes (e.g., 9,1,1)",
    }, param_type="job")
    n_trajec = required["n_trajec"]
    trajL = required["trajL"]
    lvl_sizes = required["lvl_sizes"]

    work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
    log_dir = work_root / "cnfg" / "jlog"
    script_dir = work_root / "cnfg" / "slurm"
    script_dir.mkdir(parents=True, exist_ok=True)
    volume = _format_volume(physics)
    ens_name = _format_ensemble_name(physics)
    mpi = job_params.get("mpi", DEFAULT_GPU_MPI)
    cfg_max = job_params.get("cfg_max")
    gres = job_params.get("gres")
    ntasks_per_node = int(job_params.get("ntasks_per_node", 1))
    if not gres:
        gres = f"gpu:{ntasks_per_node}"

    context = {
        # Shared SBATCH header fields
        "account": job_params.get("account"),
        "constraint": job_params.get("constraint", "gpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "06:00:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "ntasks_per_node": ntasks_per_node,
        "cpus_per_task": int(job_params.get("cpus_per_task", 32)),
        "gpus_per_task": int(job_params.get("gpus_per_task", 1)),
        "gpu_bind": job_params.get("gpu_bind", "none"),
        "gres": gres,
        "mail_user": job_params.get("mail_user") or "",
        "mail_type": "BEGIN,END",
        "signal": "B:TERM@60",
        "log_dir": str(log_dir),
        "separate_error_log": True,
        "job_name": _resolve_job_name(job_params, ensemble_id),
        # Script-specific values
        "db_file": backend.connection_string,
        "ensemble_id": ensemble_id,
        "mode": job_params.get("mode", "tepid"),
        "ensemble_name": ens_name,
        "ensemble_relpath": job_params.get("ensemble_relpath", ""),
        "volume": volume,
        "exec_path": exec_path,
        "bind_script": bind_script,
        "n_trajec": int(n_trajec),
        "mpi": mpi,
        "trajL": str(trajL),
        "lvl_sizes": str(lvl_sizes),
        "work_root": str(work_root),
        "ensemble_dir": str(Path(ensemble["directory"]).resolve()),
        "cfg_max": int(cfg_max) if cfg_max not in (None, "") else None,
        "logfile": DEFAULT_LOGFILE,
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "omp_num_threads": int(job_params.get("omp_num_threads", 16)),
        "_output_dir": str(script_dir),
        "_output_prefix": f"hmc_gpu_{job_params.get('config_start', 0)}_{job_params.get('config_end', 100)}",
    }
    return context


def build_hmc_cpu_context(backend, ensemble_id: int, job_params: Dict, input_params: Dict) -> Dict:
    """Return context for the CPU SLURM template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)
    ensure_keys(physics, ["L", "T", "beta", "b", "Ls", "ml", "ms", "mc"])

    paths = ensemble.get("paths", {})
    exec_path = job_params.get("exec_path") or paths.get("hmc_exec_path")
    if not exec_path:
        raise ValidationError("exec_path is required (set via CLI or ensemble paths.hmc_exec_path)")

    bind_script = job_params.get("bind_script") or paths.get("hmc_bind_script_cpu")
    if not bind_script:
        raise ValidationError(
            "bind_script is required (set via CLI or ensemble paths.hmc_bind_script_cpu)"
        )

    required = require_all(job_params, {
        "n_trajec": "Number of trajectories per job",
        "trajL": "Trajectory length",
        "lvl_sizes": "Level sizes (e.g., 9,1,1)",
    }, param_type="job")
    n_trajec = required["n_trajec"]
    trajL = required["trajL"]
    lvl_sizes = required["lvl_sizes"]

    work_root = Path(job_params.get("run_dir") or ensemble["directory"]).resolve()
    log_dir = work_root / "cnfg" / "jlog"
    script_dir = work_root / "cnfg" / "slurm"
    script_dir.mkdir(parents=True, exist_ok=True)
    volume = _format_volume(physics)
    ens_name = _format_ensemble_name(physics)
    mpi = job_params.get("mpi", DEFAULT_CPU_MPI)
    cacheblocking = job_params.get("cacheblocking", DEFAULT_CPU_CACHEBLOCKING)
    cfg_max = job_params.get("cfg_max")

    context = {
        "account": job_params.get("account"),
        "constraint": job_params.get("constraint", "cpu"),
        "queue": job_params.get("queue", "regular"),
        "time_limit": job_params.get("time_limit", "06:00:00"),
        "nodes": int(job_params.get("nodes", 1)),
        "ntasks_per_node": int(job_params.get("ntasks_per_node", 1)),
        "cpus_per_task": int(job_params.get("cpus_per_task", 32)),
        "mail_user": job_params.get("mail_user") or "",
        "mail_type": "BEGIN,END",
        "signal": "B:TERM@60",
        "log_dir": str(log_dir),
        "separate_error_log": True,
        "job_name": _resolve_job_name(job_params, ensemble_id),
        "db_file": backend.connection_string,
        "ensemble_id": ensemble_id,
        "mode": job_params.get("mode", "tepid"),
        "ensemble_name": ens_name,
        "ensemble_relpath": job_params.get("ensemble_relpath", ""),
        "volume": volume,
        "exec_path": exec_path,
        "bind_script": bind_script,
        "n_trajec": int(n_trajec),
        "mpi": mpi,
        "cacheblocking": cacheblocking,
        "trajL": str(trajL),
        "lvl_sizes": str(lvl_sizes),
        "work_root": str(work_root),
        "ensemble_dir": str(Path(ensemble["directory"]).resolve()),
        "cfg_max": int(cfg_max) if cfg_max not in (None, "") else None,
        "logfile": DEFAULT_LOGFILE,
        "conda_env": job_params.get("conda_env", DEFAULT_CONDA_ENV),
        "omp_num_threads": int(job_params.get("omp_num_threads", 4)),
        "_output_dir": str(script_dir),
        "_output_prefix": f"hmc_cpu_{job_params.get('config_start', 0)}_{job_params.get('config_end', 100)}",
    }
    return context


# --------------------------------------------------------------------------- #
# HMC XML support (will be used by input templates)
# --------------------------------------------------------------------------- #

def build_hmc_xml_context(backend, ensemble_id: int, input_params: Dict) -> Dict:
    """Build context for rendering HMCparameters.xml."""
    mode = input_params.get("mode", "tepid")
    seed_override = input_params.get("Seed")
    overrides = {k: v for k, v in input_params.items() if k not in {"mode", "Seed"}}

    tree, root = _make_default_tree(mode, _maybe_int(seed_override))
    _apply_xml_overrides(root, overrides)
    xml_string = _tree_to_string(tree)
    
    # Get ensemble directory for output placement
    ensemble = get_ensemble_doc(backend, ensemble_id)
    ensemble_dir = Path(ensemble["directory"]).resolve()
    xml_dir = ensemble_dir / "cnfg"
    xml_dir.mkdir(parents=True, exist_ok=True)
    
    return {
        "xml": xml_string,
        "_output_dir": str(xml_dir),
        "_output_prefix": "HMCparameters",
    }


# --------------------------------------------------------------------------- #
# Legacy XML helpers retained for migration scripts
# --------------------------------------------------------------------------- #

def _make_default_tree(mode: str, seed_override: Optional[int] = None):
    """Build a fresh ElementTree with defaults for tepid/continue/reseed."""
    seed = seed_override if seed_override is not None else random.randint(1, 10**6)

    if mode == "tepid":
        start, traj = 0, 100
        stype, metropolis = "TepidStart", False
    elif mode == "continue":
        start, traj = 12, 20
        stype, metropolis = "CheckpointStart", True
    elif mode == "reseed":
        start, traj = 0, 200
        stype, metropolis = "CheckpointStartReseed", True
    else:
        raise ValueError(f"Unknown mode '{mode}'")

    md_name = ["OMF2_5StepV", "OMF2_5StepV", "OMF4_11StepV"]
    md_steps = 1
    trajL = 0.75
    lvl_sizes = [9, 1, 1]

    root = ET.Element("grid")
    hmc = ET.SubElement(root, "HMCparameters")

    def elem(tag, value):
        ET.SubElement(hmc, tag).text = str(value)

    elem("StartTrajectory", start)
    elem("Trajectories", traj)
    elem("MetropolisTest", str(metropolis).lower())
    elem("NoMetropolisUntil", 0)
    elem("PerformRandomShift", "false")
    elem("StartingType", stype)
    elem("Seed", seed)

    md = ET.SubElement(hmc, "MD")
    names = ET.SubElement(md, "name")
    for entry in md_name:
        ET.SubElement(names, "elem").text = entry

    ET.SubElement(md, "MDsteps").text = str(md_steps)
    ET.SubElement(md, "trajL").text = str(trajL)

    levels = ET.SubElement(md, "lvl_sizes")
    for value in lvl_sizes:
        ET.SubElement(levels, "elem").text = str(value)

    return ET.ElementTree(root), root


def _pretty_write(tree: ET.ElementTree, path: Path):
    """Pretty-print XML with indentation and write to path."""
    raw = ET.tostring(tree.getroot(), encoding="utf-8")
    parsed = minidom.parseString(raw)
    pretty = parsed.toprettyxml(indent="  ")
    lines = [ln for ln in pretty.splitlines() if ln.strip()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _tree_to_string(tree: ET.ElementTree) -> str:
    raw = ET.tostring(tree.getroot(), encoding="utf-8")
    parsed = minidom.parseString(raw)
    pretty = parsed.toprettyxml(indent="  ")
    lines = [ln for ln in pretty.splitlines() if ln.strip()]
    return "\n".join(lines) + "\n"


def _apply_xml_overrides(root: ET.Element, overrides: Dict) -> None:
    """Apply user overrides to the HMC XML tree."""
    for key, value in overrides.items():
        text = str(value)
        if key == "md_name":
            names = root.find("MD/name")
            if names is None:
                continue
            names.clear()
            for entry in text.split(","):
                ET.SubElement(names, "elem").text = entry.strip()
            continue
        if key == "lvl_sizes":
            levels = root.find("MD/lvl_sizes")
            if levels is None:
                continue
            levels.clear()
            for entry in text.split(","):
                ET.SubElement(levels, "elem").text = entry.strip()
            continue

        node = root.find(key)
        if node is not None:
            node.text = text
            continue

        md_node = root.find(f"MD/{key}")
        if md_node is not None:
            md_node.text = text


def _maybe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #

def _format_volume(physics: Dict) -> str:
    L = int(physics["L"])
    T = int(physics["T"])
    return f"{L}.{L}.{L}.{T}"


def _format_ensemble_name(physics: Dict) -> str:
    ensure_keys(physics, ["beta", "b", "Ls", "mc", "ms", "ml", "L", "T"])
    return (
        f"b{physics['beta']}_b{physics['b']}"
        f"Ls{physics['Ls']}_mc{physics['mc']}_ms{physics['ms']}_ml{physics['ml']}"
        f"_L{physics['L']}_T{physics['T']}"
    )


def _resolve_job_name(job_params: Dict, ensemble_id: int) -> str:
    if job_params.get("job_name"):
        return job_params["job_name"]
    if job_params.get("nickname"):
        return f"HMC_{job_params['nickname']}"
    return f"HMC_e{ensemble_id}"


def _require(params: Dict, key: str, message: str):
    value = params.get(key)
    if value in (None, ""):
        raise ValidationError(message)
    return value

