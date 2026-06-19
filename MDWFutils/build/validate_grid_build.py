"""Validation for grid_build parameters before rendering Nf2p1p1.cc."""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from ..exceptions import ValidationError
from .grid_catalog import BETA_LINES, eofa_hs_masses


def validate_grid_build(
    physics: Dict,
    grid_build: Dict,
    *,
    force_physics_mismatch: bool = False,
) -> List[str]:
    """Validate grid_build against physics and catalog. Returns warnings."""
    if not grid_build:
        raise ValidationError("grid_build is missing; run 'mdwf_db build grid init -e <ensemble>' first")

    required = (
        "beta_line",
        "light_mass",
        "hasenbusch",
        "nlvl1",
        "eofa_integrator_level",
        "charm_mass_factor",
        "pv_mass",
    )
    missing = [k for k in required if grid_build.get(k) in (None, "")]
    if missing:
        raise ValidationError(f"grid_build missing required keys: {missing}")

    beta_line = grid_build["beta_line"]
    if beta_line not in BETA_LINES:
        raise ValidationError(f"Unknown beta_line '{beta_line}'")

    spec = BETA_LINES[beta_line]
    L = int(physics["L"])
    if L not in spec.supported_L:
        raise ValidationError(f"L={L} not supported for beta line {beta_line}")

    warnings: List[str] = []
    checks = {
        "beta": (float(physics["beta"]), spec.beta),
        "b": (float(physics["b"]), spec.b),
        "Ls": (int(physics["Ls"]), spec.Ls),
        "ms": (float(physics["ms"]), spec.ms),
    }
    for key, (actual, expected) in checks.items():
        if abs(actual - expected) > 1e-4:
            msg = f"physics.{key}={actual} does not match catalog {beta_line} ({expected})"
            if force_physics_mismatch:
                warnings.append(msg)
            else:
                raise ValidationError(f"{msg}; use --force-physics-mismatch to override")

    hasenbusch = grid_build["hasenbusch"]
    if not isinstance(hasenbusch, list) or not hasenbusch:
        raise ValidationError("hasenbusch must be a non-empty list")
    if any(m <= 0 for m in hasenbusch):
        raise ValidationError("hasenbusch masses must be positive")
    if float(grid_build["light_mass"]) <= 0:
        raise ValidationError("light_mass must be positive")

    nlvl1 = int(grid_build["nlvl1"])
    if nlvl1 < 0 or nlvl1 > len(hasenbusch):
        raise ValidationError(f"nlvl1={nlvl1} must be in [0, {len(hasenbusch)}]")

    if beta_line == "b4333" and grid_build.get("eofa_hs_extra") is None:
        raise ValidationError("b4333 requires grid_build.eofa_hs_extra")

    eofa_level = int(grid_build["eofa_integrator_level"])
    if eofa_level not in (1, 2):
        raise ValidationError("eofa_integrator_level must be 1 or 2")

    return warnings


def merge_physics_and_grid_build(physics: Dict, grid_build: Dict) -> Dict:
    """Build template context for Nf2p1p1.cc from physics + grid_build."""
    beta_line = grid_build["beta_line"]
    spec = BETA_LINES[beta_line]
    ms = float(physics["ms"])
    charm_factor = float(grid_build["charm_mass_factor"])
    return {
        "beta_line": beta_line,
        "Ls": int(physics["Ls"]),
        "beta": float(physics["beta"]),
        "ms": ms,
        "b": float(physics["b"]),
        "c": spec.c,
        "light_mass": float(grid_build["light_mass"]),
        "hasenbusch": [float(x) for x in grid_build["hasenbusch"]],
        "nlvl1": int(grid_build["nlvl1"]),
        "eofa_integrator_level": int(grid_build["eofa_integrator_level"]),
        "pv_mass": float(grid_build["pv_mass"]),
        "charm_mass_factor": charm_factor,
        "charm_mass": charm_factor * ms,
        "eofa_hs_extra": grid_build.get("eofa_hs_extra"),
        "save_interval": spec.save_interval,
        "L": int(physics["L"]),
        "eofa_hs": eofa_hs_masses(beta_line, ms, charm_factor * ms),
    }
