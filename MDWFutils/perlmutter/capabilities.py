"""Read-only NERSC/Perlmutter capability detection."""

from __future__ import annotations

import os
import shlex
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Dict, List


TOOLS = [
    "sbatch",
    "squeue",
    "sacct",
    "scontrol",
    "sqs",
    "jobstats",
    "seff",
    "showquota",
    "lfs",
    "globus",
    "hsi",
    "htar",
    "podman-hpc",
    "shifter",
    "module",
    "ml",
    "python",
    "python3",
    "git",
    "uv",
    "rsync",
]


def collect_capabilities() -> Dict:
    hostname = socket.gethostname()
    env = {key: os.getenv(key) for key in _interesting_env_keys() if os.getenv(key)}
    tools = {tool: _find_tool(tool) for tool in TOOLS}
    filesystems = _filesystem_paths()
    on_perlmutter = _is_perlmutter(hostname, env)
    warnings = []
    if not on_perlmutter:
        warnings.append("This shell does not look like a Perlmutter/NERSC environment.")
    if not tools.get("sbatch"):
        warnings.append("Slurm submit command sbatch was not found.")
    if not filesystems.get("scratch"):
        warnings.append("No PSCRATCH/SCRATCH path detected.")
    return {
        "ok": True,
        "on_perlmutter": on_perlmutter,
        "hostname": hostname,
        "env": env,
        "tools": tools,
        "filesystems": filesystems,
        "quotas": _quota_summary(tools),
        "recommendations": _recommendations(tools, filesystems, on_perlmutter),
        "warnings": warnings,
    }


def _interesting_env_keys() -> List[str]:
    prefixes = ("NERSC", "SLURM", "SCRATCH", "PSCRATCH", "CFS", "HOME", "USER")
    keys = []
    for key in os.environ:
        if key.startswith(prefixes) or key in ("HOST", "HOSTNAME"):
            keys.append(key)
    return sorted(keys)


def _filesystem_paths() -> Dict[str, str | None]:
    return {
        "scratch": os.getenv("PSCRATCH") or os.getenv("SCRATCH"),
        "cfs": os.getenv("CFS"),
        "home": os.getenv("HOME"),
    }


def _is_perlmutter(hostname: str, env: Dict[str, str]) -> bool:
    host = hostname.lower()
    nersc_host = (env.get("NERSC_HOST") or "").lower()
    cluster = (env.get("SLURM_CLUSTER_NAME") or "").lower()
    return "perlmutter" in host or nersc_host == "perlmutter" or cluster == "perlmutter"


def _find_tool(tool: str) -> str | None:
    path = shutil.which(tool)
    if path:
        return path
    if tool not in {"module", "ml"} or not shutil.which("bash"):
        return None
    result = subprocess.run(
        ["bash", "-lc", f"type -t {shlex.quote(tool)}"],
        check=False,
        text=True,
        capture_output=True,
    )
    kind = result.stdout.strip()
    return f"shell:{kind}" if result.returncode == 0 and kind else None


def _quota_summary(tools: Dict[str, str | None]) -> Dict:
    if not tools.get("showquota"):
        return {"available": False}
    result = subprocess.run(
        ["showquota", "--json"],
        check=False,
        text=True,
        capture_output=True,
    )
    return {
        "available": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _recommendations(tools: Dict[str, str | None], filesystems: Dict[str, str | None], on_perlmutter: bool) -> List[str]:
    recs = []
    if on_perlmutter and filesystems.get("scratch"):
        recs.append("Use PSCRATCH/SCRATCH for active ensemble compute data.")
    if on_perlmutter and filesystems.get("cfs"):
        recs.append("Use CFS for shared code, configuration, and curated durable outputs.")
    if tools.get("sacct") and tools.get("squeue"):
        recs.append("Use batched squeue/sacct monitor calls instead of tight polling loops.")
    if tools.get("jobstats"):
        recs.append("Attach jobstats summaries to completed operation records.")
    if tools.get("podman-hpc"):
        recs.append("Prefer podman-hpc profiles for reproducible user-space workflows when practical.")
    elif tools.get("shifter"):
        recs.append("Use Shifter as a container fallback when podman-hpc is unavailable.")
    return recs
