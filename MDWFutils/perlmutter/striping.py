"""Lustre striping planning helpers."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional


def stripe_plan(path: str, mode: str = "default", nodes: Optional[int] = None) -> Dict:
    target = Path(path).expanduser()
    current = _getstripe(target)
    recommendation = _recommendation(mode, nodes)
    warnings = []
    if not shutil.which("lfs"):
        warnings.append("lfs is not available in this environment.")
    if mode == "many-small-files":
        warnings.append("Keep default striping for many small file-per-config workloads.")
    return {
        "path": str(target),
        "exists": target.exists(),
        "mode": mode,
        "nodes": nodes,
        "current": current,
        "recommendation": recommendation,
        "apply_command": _apply_command(target, recommendation),
        "warnings": warnings,
    }


def apply_stripe(path: str, mode: str = "default", nodes: Optional[int] = None) -> Dict:
    plan = stripe_plan(path, mode=mode, nodes=nodes)
    command = plan["apply_command"]
    if not command:
        return {**plan, "applied": False, "reason": "No striping change recommended"}
    if not shutil.which("lfs"):
        return {**plan, "applied": False, "reason": "lfs command not available"}
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    return {
        **plan,
        "applied": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _getstripe(path: Path) -> Dict:
    if not shutil.which("lfs") or not path.exists():
        return {"available": False}
    result = subprocess.run(["lfs", "getstripe", str(path)], check=False, text=True, capture_output=True)
    return {
        "available": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _recommendation(mode: str, nodes: Optional[int]) -> Dict:
    if mode in ("default", "many-small-files"):
        return {"stripe_count": None, "reason": "Use filesystem default striping."}
    if mode in ("large-hdf5", "parallel-hdf5"):
        count = max(1, min(8, int(nodes or 4)))
        return {"stripe_count": count, "reason": "Large shared files benefit from moderate striping before creation."}
    return {"stripe_count": None, "reason": f"Unknown mode '{mode}', no change recommended."}


def _apply_command(path: Path, recommendation: Dict):
    count = recommendation.get("stripe_count")
    if not count:
        return None
    return ["lfs", "setstripe", "-c", str(count), str(path)]
