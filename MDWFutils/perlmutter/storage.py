"""NERSC storage classification and planning."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional


def classify_path(path: str) -> Dict:
    resolved = Path(path).expanduser()
    try:
        resolved_str = str(resolved.resolve())
    except FileNotFoundError:
        resolved_str = str(resolved.absolute())
    scratch = _norm(os.getenv("PSCRATCH") or os.getenv("SCRATCH"))
    cfs = _norm(os.getenv("CFS"))
    home = _norm(os.getenv("HOME"))
    path_class = "unknown"
    warnings = []
    recommendations = []
    if scratch and _under(resolved_str, scratch):
        path_class = "scratch_active"
        warnings.append("NERSC scratch is purgeable; keep durable copies of important outputs.")
        recommendations.append("Use this for active ensemble compute and high-turnover files.")
    elif cfs and _under(resolved_str, cfs):
        path_class = "cfs_project"
        recommendations.append("Use this for shared code/configuration and curated durable outputs.")
        warnings.append("Avoid heavy compute I/O directly from CFS when PSCRATCH is suitable.")
    elif home and _under(resolved_str, home):
        path_class = "home_config"
        warnings.append("Home is appropriate for lightweight config, not ensemble data.")
    return {
        "path": resolved_str,
        "class": path_class,
        "exists": resolved.exists(),
        "warnings": warnings,
        "recommendations": recommendations,
    }


def storage_plan(path: Optional[str] = None, ensemble: Optional[Dict] = None) -> Dict:
    target = path or (ensemble or {}).get("directory")
    if not target:
        raise ValueError("storage plan requires a path or ensemble with directory")
    classified = classify_path(target)
    archive_candidates = []
    if os.getenv("CFS"):
        archive_candidates.append({"type": "cfs", "base": os.getenv("CFS")})
    archive_candidates.append({"type": "globus", "note": "Use Globus for endpoint-to-endpoint transfer plans."})
    archive_candidates.append({"type": "hpss", "note": "Use hsi/htar for archival copies when available."})
    return {
        "active": classified,
        "archive_candidates": archive_candidates,
        "policy": {
            "active_compute": "scratch_active",
            "durable_outputs": "cfs_project",
            "archive": "hpss_or_globus",
        },
    }


def _norm(path: Optional[str]) -> Optional[str]:
    return str(Path(path).expanduser().resolve()) if path else None


def _under(path: str, parent: str) -> bool:
    try:
        Path(path).relative_to(parent)
        return True
    except ValueError:
        return False
