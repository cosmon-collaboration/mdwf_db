"""Context builders for HMC scripts and inputs."""

from __future__ import annotations

import random
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Optional
from xml.dom import minidom

from MDWFutils.exceptions import ValidationError

from .schema import ContextBuilder, ContextParam, common_slurm_params, DEFAULT_CONDA_ENV
from .utils import (
    compute_kappa,
    ensure_keys,
    require_all,
)
DEFAULT_LOGFILE = "/global/cfs/cdirs/m2986/cosmon/mdwf/mdwf_update.log"
DEFAULT_GPU_MPI = "4.4.4.8"
DEFAULT_CPU_MPI = "4.4.4.8"
DEFAULT_CPU_CACHEBLOCKING = "2.2.2.2"
HMC_MODE_CHOICES = ["tepid", "continue", "reseed"]


def _hmc_run_input_params():
    """Executable/run parameters for HMC SLURM scripts (passed via -i)."""
    return [
        ContextParam("n_trajec", int, required=True, aliases=["Trajectories"], help="Number of trajectories per job"),
        ContextParam("trajL", float, required=True, help="Trajectory length"),
        ContextParam("lvl_sizes", str, required=True, help="Level sizes (e.g., 9,1,1)"),
        ContextParam(
            "mode",
            str,
            default="tepid",
            choices=HMC_MODE_CHOICES,
            help="HMC mode (tepid/continue/reseed)",
        ),
        ContextParam("ensemble_relpath", str, default="", help="Ensemble relative path"),
        ContextParam("config_start", int, aliases=["StartTrajectory"], storable=False, help="First configuration (for output prefix)"),
        ContextParam("config_end", int, storable=False, help="Last configuration (for output prefix)"),
    ]


# --------------------------------------------------------------------------- #
# HMC GPU / CPU context builders
# --------------------------------------------------------------------------- #

class HMCGPUContextBuilder(ContextBuilder):
    """HMC GPU job context builder"""
    
    type_name = "hmc_gpu"
    
    job_params_schema = [
        *common_slurm_params(),
        # Override for GPU jobs
        ContextParam("constraint", str, default="gpu", help="Node constraint"),
        ContextParam("time_limit", str, default="06:00:00", help="SLURM time limit"),
        ContextParam("cpus_per_task", int, default=32, help="CPUs per task"),
        ContextParam("mail_type", str, default="BEGIN,END", help="Mail notification types"),
        # GPU-specific params
        ContextParam("ntasks_per_node", int, default=4, help="Tasks per node (legacy default 4)"),
        ContextParam("gpus_per_task", int, default=1, help="GPUs per task"),
        ContextParam("gpu_bind", str, default="none", help="GPU binding policy"),
        ContextParam("gres", str, storable=False, help="GPU resource specification (auto-generated if not provided)"),
        # HMC-specific params
        ContextParam("run_dir", str, storable=False, help="Working directory (defaults to ensemble directory)"),
        ContextParam("exec_path", str, storable=False, help="HMC executable path (or set via ensemble paths.hmc_exec_path)"),
        ContextParam("bind_script", str, storable=False, help="CPU binding script (or set via ensemble paths.hmc_bind_script_gpu)"),
        ContextParam("mpi", str, default=DEFAULT_GPU_MPI, help="MPI configuration"),
        ContextParam("cfg_max", int, storable=False, help="Maximum configuration number"),
        ContextParam("conda_env", str, default=DEFAULT_CONDA_ENV, help="Conda environment path"),
        ContextParam("omp_num_threads", int, default=16, help="OpenMP threads"),
    ]
    
    input_params_schema = _hmc_run_input_params()
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Return context for the GPU SLURM template."""
        ensure_keys(physics, ["L", "T", "beta", "b", "Ls", "ml", "ms", "mc"])

        paths = resolve_hmc_paths(ensemble.get("hmc_paths", {}))
        exec_path = job_params.get("exec_path") or paths.get("exec_path")
        if not exec_path:
            raise ValidationError("exec_path is required (set via CLI or ensemble hmc_paths.exec_path)")

        bind_script = job_params.get("bind_script") or paths.get("bind_script_gpu")
        if not bind_script:
            raise ValidationError(
                "bind_script is required (set via CLI or ensemble paths.hmc_bind_script_gpu)"
            )

        # Extract values needed for computations
        n_trajec = input_params["n_trajec"]
        trajL = input_params["trajL"]
        lvl_sizes = input_params["lvl_sizes"]

        work_root = self._resolve_run_dir(ensemble, job_params)
        log_dir = work_root / "cnfg" / "jlog"
        script_dir = work_root / "cnfg" / "slurm"
        script_dir.mkdir(parents=True, exist_ok=True)
        volume = _format_volume(physics)
        ens_name = _format_ensemble_name(physics)
        
        # Compute gres if not provided
        ntasks_per_node = job_params["ntasks_per_node"]
        gres = job_params.get("gres") or f"gpu:{ntasks_per_node}"
        cfg_max = job_params.get("cfg_max")

        # Return ONLY computed/special values
        return {
            "ntasks_per_node": ntasks_per_node,
            "gres": gres,
            "mail_type": "BEGIN,END",
            "signal": "B:TERM@60",
            "log_dir": str(log_dir),
            "separate_error_log": True,
            "job_name": _resolve_job_name(job_params, ensemble_id),
            "ensemble_id": ensemble_id,
            "ensemble_name": ens_name,
            "volume": volume,
            "exec_path": exec_path,
            "bind_script": bind_script,
            "n_trajec": int(n_trajec),
            "trajL": str(trajL),
            "lvl_sizes": str(lvl_sizes),
            "work_root": str(work_root),
            "ensemble_dir": str(Path(ensemble["directory"]).resolve()),
            "cfg_max": int(cfg_max) if cfg_max not in (None, "") else None,
            "logfile": DEFAULT_LOGFILE,
            "_output_dir": str(script_dir),
            "_output_prefix": (
                f"hmc_gpu_{input_params.get('config_start', 0)}_{input_params.get('config_end', 100)}"
            ),
        }


class HMCCPUContextBuilder(ContextBuilder):
    """HMC CPU job context builder"""
    
    type_name = "hmc_cpu"
    
    job_params_schema = [
        *common_slurm_params(),
        # Override for CPU jobs
        ContextParam("constraint", str, default="cpu", help="Node constraint"),
        ContextParam("time_limit", str, default="06:00:00", help="SLURM time limit"),
        ContextParam("cpus_per_task", int, default=32, help="CPUs per task"),
        ContextParam("mail_type", str, default="BEGIN,END", help="Mail notification types"),
        # CPU-specific params
        ContextParam("ntasks_per_node", int, default=1, help="Tasks per node"),
        ContextParam("cacheblocking", str, default=DEFAULT_CPU_CACHEBLOCKING, help="Cache blocking configuration"),
        # HMC-specific params
        ContextParam("run_dir", str, storable=False, help="Working directory (defaults to ensemble directory)"),
        ContextParam("exec_path", str, storable=False, help="HMC executable path (or set via ensemble paths.hmc_exec_path)"),
        ContextParam("bind_script", str, storable=False, help="CPU binding script (or set via ensemble paths.hmc_bind_script_cpu)"),
        ContextParam("mpi", str, default=DEFAULT_CPU_MPI, help="MPI configuration"),
        ContextParam("cfg_max", int, storable=False, help="Maximum configuration number"),
        ContextParam("conda_env", str, default=DEFAULT_CONDA_ENV, help="Conda environment path"),
        ContextParam("omp_num_threads", int, default=4, help="OpenMP threads"),
    ]
    
    input_params_schema = _hmc_run_input_params()
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Return context for the CPU SLURM template."""
        ensure_keys(physics, ["L", "T", "beta", "b", "Ls", "ml", "ms", "mc"])

        paths = resolve_hmc_paths(ensemble.get("hmc_paths", {}))
        exec_path = job_params.get("exec_path") or paths.get("exec_path")
        if not exec_path:
            raise ValidationError("exec_path is required (set via CLI or ensemble hmc_paths.exec_path)")

        bind_script = job_params.get("bind_script") or paths.get("bind_script_cpu")
        if not bind_script:
            raise ValidationError(
                "bind_script is required (set via CLI or ensemble paths.hmc_bind_script_cpu)"
            )

        # Extract values needed for computations
        n_trajec = input_params["n_trajec"]
        trajL = input_params["trajL"]
        lvl_sizes = input_params["lvl_sizes"]

        work_root = self._resolve_run_dir(ensemble, job_params)
        log_dir = work_root / "cnfg" / "jlog"
        script_dir = work_root / "cnfg" / "slurm"
        script_dir.mkdir(parents=True, exist_ok=True)
        volume = _format_volume(physics)
        ens_name = _format_ensemble_name(physics)
        cfg_max = job_params.get("cfg_max")

        # Return ONLY computed/special values
        return {
            "mail_type": "BEGIN,END",
            "signal": "B:TERM@60",
            "log_dir": str(log_dir),
            "separate_error_log": True,
            "job_name": _resolve_job_name(job_params, ensemble_id),
            "ensemble_id": ensemble_id,
            "ensemble_name": ens_name,
            "volume": volume,
            "exec_path": exec_path,
            "bind_script": bind_script,
            "n_trajec": int(n_trajec),
            "trajL": str(trajL),
            "lvl_sizes": str(lvl_sizes),
            "work_root": str(work_root),
            "ensemble_dir": str(Path(ensemble["directory"]).resolve()),
            "cfg_max": int(cfg_max) if cfg_max not in (None, "") else None,
            "logfile": DEFAULT_LOGFILE,
            "_output_dir": str(script_dir),
            "_output_prefix": (
                f"hmc_cpu_{input_params.get('config_start', 0)}_{input_params.get('config_end', 100)}"
            ),
        }


# --------------------------------------------------------------------------- #
# HMC XML support (will be used by input templates)
# --------------------------------------------------------------------------- #

class HMCXMLContextBuilder(ContextBuilder):
    """HMC XML input file context builder."""
    
    type_name = "hmc_xml"
    
    input_params_schema = [
        ContextParam(
            "mode",
            str,
            default="tepid",
            choices=HMC_MODE_CHOICES,
            help="HMC start mode (tepid/continue/reseed)",
        ),
        ContextParam("Seed", int, help="Random seed (optional)"),
        ContextParam(
            "Trajectories",
            int,
            required=True,
            aliases=["n_trajec"],
            help="Number of trajectories (HMC XML tag)",
        ),
        ContextParam("trajL", float, required=True, help="Trajectory length"),
        ContextParam("MDsteps", int, help="MD steps"),
        ContextParam("md_name", str, help="MD integrator names (comma-separated)"),
        ContextParam("MetropolisTest", str, help="Metropolis test (true/false)"),
        ContextParam("NoMetropolisUntil", int, help="Trajectory to start Metropolis"),
        ContextParam("PerformRandomShift", str, help="Perform random shift (true/false)"),
        ContextParam("StartingType", str, help="Starting type override"),
        ContextParam(
            "StartTrajectory",
            int,
            aliases=["config_start"],
            help="Starting trajectory (HMC XML tag)",
        ),
        ContextParam("lvl_sizes", str, help="Level sizes override (comma-separated)"),
    ]
    
    def _build_context(self, backend, ensemble_id: int, ensemble: Dict, physics: Dict,
                      job_params: Dict, input_params: Dict) -> Dict:
        """Build HMC XML. Schema params auto-merged."""
        mode = input_params["mode"]
        seed_override = input_params.get("Seed")  # optional, no default
        trajectories = input_params["Trajectories"]
        traj_l = input_params["trajL"]

        tree, root = _make_default_tree(mode, _maybe_int(seed_override))
        overrides = {"Trajectories": trajectories, "trajL": traj_l}

        start_traj = input_params.get("StartTrajectory")
        if start_traj is not None:
            overrides["StartTrajectory"] = start_traj

        for key in (
            "MDsteps",
            "MetropolisTest",
            "NoMetropolisUntil",
            "PerformRandomShift",
            "StartingType",
            "lvl_sizes",
        ):
            if key in input_params and input_params[key] is not None:
                overrides[key] = input_params[key]
        if "md_name" in input_params and input_params["md_name"] is not None:
            overrides["md_name"] = input_params["md_name"]

        _apply_xml_overrides(root, overrides)
        xml_string = _tree_to_string(tree)
        
        ensemble_dir = Path(ensemble["directory"]).resolve()
        xml_dir = ensemble_dir / "cnfg"
        xml_dir.mkdir(parents=True, exist_ok=True)
        
        return {
            "xml": xml_string,
            "_output_dir": str(xml_dir),
            "_output_prefix": "HMCparameters",
            "_output_suffix": ".xml",
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


def _hmc_parameters_element(root: ET.Element) -> Optional[ET.Element]:
    """Return the HMCparameters element from a grid or HMCparameters root."""
    if root.tag == "HMCparameters":
        return root
    return root.find("HMCparameters")


def _apply_xml_overrides(root: ET.Element, overrides: Dict) -> None:
    """Apply user overrides to the HMC XML tree."""
    hmc = _hmc_parameters_element(root)
    if hmc is None:
        return

    for key, value in overrides.items():
        text = str(value)
        if key == "md_name":
            names = hmc.find("MD/name")
            if names is None:
                continue
            names.clear()
            for entry in text.split(","):
                ET.SubElement(names, "elem").text = entry.strip()
            continue
        if key == "lvl_sizes":
            levels = hmc.find("MD/lvl_sizes")
            if levels is None:
                continue
            levels.clear()
            for entry in text.split(","):
                ET.SubElement(levels, "elem").text = entry.strip()
            continue

        node = hmc.find(key)
        if node is not None:
            node.text = text
            continue

        md_node = hmc.find(f"MD/{key}")
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


def resolve_hmc_paths(paths: Optional[Dict]) -> Dict[str, Optional[str]]:
    """Return canonical HMC paths, honoring legacy stored key names."""
    paths = paths or {}
    return {
        "exec_path": paths.get("exec_path") or paths.get("hmc_exec_path"),
        "bind_script_gpu": (
            paths.get("bind_script_gpu")
            or paths.get("hmc_bind_script_gpu")
            or paths.get("hmc_bind_script")
        ),
        "bind_script_cpu": (
            paths.get("bind_script_cpu")
            or paths.get("hmc_bind_script_cpu")
            or paths.get("hmc_bind_script")
        ),
    }


HMC_PATH_STATUS_FIELDS = (
    ("exec_path", "exec_path"),
    ("bind_script_gpu", "bind_script_gpu"),
    ("bind_script_cpu", "bind_script_cpu"),
)


def format_hmc_path_status_value(value: Optional[str]) -> str:
    """Format a stored HMC path for status output."""
    return value if value else "NONE"


def _require(params: Dict, key: str, message: str):
    value = params.get(key)
    if value in (None, ""):
        raise ValidationError(message)
    return value

