"""Action string and install path helpers for Grid HMC builds."""

from __future__ import annotations

from typing import Dict

from ..jobs.hmc import _format_ensemble_name


def format_action(physics: Dict) -> str:
    """Format ensemble action string from physics fields."""
    return _format_ensemble_name(physics)


def grid_build_dir_name(action: str, gpu: bool) -> str:
    """Return isolated Grid build directory name under build_dir."""
    if gpu:
        return f"Grid_{action}_gpu"
    return f"Grid_{action}"


def grid_install_prefix(install_root: str, action: str) -> str:
    """Return Grid install prefix (no _gpu suffix on install)."""
    return f"{install_root.rstrip('/')}/Grid_{action}"


def grid_hmc_exec_path(install_prefix: str) -> str:
    return f"{install_prefix}/bin/Nf2p1p1"


def nf2p1p1_output_path(scripts_dir: str, action: str) -> str:
    return f"{scripts_dir.rstrip('/')}/grid_scripts/Nf2p1p1_{action}.cc"
