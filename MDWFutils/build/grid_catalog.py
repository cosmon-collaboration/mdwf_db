"""Beta-line catalog for Grid HMC Nf2p1p1 builds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class VolumeDefaults:
    light_mass: float
    hasenbusch: Tuple[float, ...]
    nlvl1: int


@dataclass(frozen=True)
class BetaLineSpec:
    name: str
    beta: float
    b: float
    Ls: int
    ms: float
    supported_L: Tuple[int, ...]
    eofa_integrator_level: int
    eofa_on_level1_for_beta_lines: Tuple[str, ...] = ()
    save_interval: int = 5
    volumes: Dict[int, VolumeDefaults] = None

    @property
    def c(self) -> float:
        return 1.0 - self.b


BETA_LINES: Dict[str, BetaLineSpec] = {
    "b4008": BetaLineSpec(
        name="b4008",
        beta=4.008,
        b=1.75,
        Ls=10,
        ms=0.0725,
        supported_L=(16, 20, 24, 32),
        eofa_integrator_level=1,
        save_interval=4,
        volumes={
            32: VolumeDefaults(0.004, (0.0075, 0.0125, 0.0225, 0.0475, 0.09, 0.18, 0.36, 0.64), 3),
            24: VolumeDefaults(0.009, (0.016, 0.028, 0.045, 0.09, 0.18, 0.4, 0.64), 2),
            20: VolumeDefaults(0.012, (0.02, 0.045, 0.08, 0.16, 0.4, 0.64), 2),
            16: VolumeDefaults(0.0195, (0.038, 0.09, 0.15, 0.3, 0.5), 1),
        },
    ),
    "b4068": BetaLineSpec(
        name="b4068",
        beta=4.068,
        b=1.5,
        Ls=8,
        ms=0.056,
        supported_L=(16, 20, 24, 32),
        eofa_integrator_level=2,
        save_interval=4,
        volumes={
            32: VolumeDefaults(0.005, (0.013, 0.03, 0.06, 0.17, 0.33, 0.63), 2),
            24: VolumeDefaults(0.010, (0.017, 0.035, 0.07, 0.17, 0.33, 0.63), 2),
            20: VolumeDefaults(0.016, (0.035, 0.07, 0.17, 0.33, 0.61), 2),
            16: VolumeDefaults(0.022, (0.04, 0.07, 0.17, 0.33, 0.61), 2),
        },
    ),
    "b416": BetaLineSpec(
        name="b416",
        beta=4.160,
        b=1.35,
        Ls=6,
        ms=0.0425,
        supported_L=(20, 24, 32),
        eofa_integrator_level=1,
        volumes={
            32: VolumeDefaults(0.006, (0.02, 0.05, 0.15, 0.5), 1),
            24: VolumeDefaults(0.012, (0.05, 0.15, 0.5), 1),
            20: VolumeDefaults(0.016, (0.06, 0.15, 0.5), 0),
        },
    ),
    "b4238": BetaLineSpec(
        name="b4238",
        beta=4.238,
        b=1.2,
        Ls=4,
        ms=0.0305,
        supported_L=(24, 32),
        eofa_integrator_level=1,
        volumes={
            32: VolumeDefaults(0.0086, (0.035, 0.14, 0.4), 0),
            24: VolumeDefaults(0.012, (0.055, 0.14, 0.4), 0),
        },
    ),
    "b4300": BetaLineSpec(
        name="b4300",
        beta=4.3,
        b=1.175,
        Ls=4,
        ms=0.0245,
        supported_L=(32, 48),
        eofa_integrator_level=1,
        volumes={
            48: VolumeDefaults(0.0035, (0.008, 0.12, 0.35), 0),
            32: VolumeDefaults(0.008, (0.03, 0.12, 0.35), 0),
        },
    ),
    "b4333": BetaLineSpec(
        name="b4333",
        beta=4.333,
        b=1.16,
        Ls=4,
        ms=0.0202,
        supported_L=(32, 48),
        eofa_integrator_level=2,
        volumes={
            48: VolumeDefaults(0.003, (0.015, 0.07, 0.15, 0.3, 0.58), 0),
            32: VolumeDefaults(0.0073, (0.015, 0.07, 0.2, 0.55), 1),
        },
    ),
}


def pick_beta_line(beta: float, tolerance: float = 0.01) -> Optional[str]:
    """Pick catalog beta line closest to physics.beta."""
    best = None
    best_delta = None
    for name, spec in BETA_LINES.items():
        delta = abs(spec.beta - beta)
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best = name
    if best is not None and best_delta <= tolerance:
        return best
    return best


def seed_grid_build(physics: Dict) -> Dict:
    """Seed grid_build dict from physics using catalog defaults."""
    beta_line = pick_beta_line(float(physics["beta"]))
    if not beta_line:
        raise ValueError(f"No beta line within tolerance for beta={physics['beta']}")
    spec = BETA_LINES[beta_line]
    L = int(physics["L"])
    if L not in spec.volumes:
        raise ValueError(f"L={L} not supported for beta line {beta_line}")
    vol = spec.volumes[L]
    result = {
        "beta_line": beta_line,
        "light_mass": vol.light_mass,
        "hasenbusch": list(vol.hasenbusch),
        "nlvl1": vol.nlvl1,
        "eofa_integrator_level": spec.eofa_integrator_level,
        "charm_mass_factor": 11.8,
        "pv_mass": 1.0,
        "eofa_hs_extra": 0.55 if beta_line == "b4333" else None,
        "notes": None,
    }
    return result


def eofa_hs_masses(beta_line: str, strange_mass: float, charm_mass: float) -> List[float]:
    """Return EOFA Hasenbusch mass ladder for beta line."""
    if beta_line == "b4008":
        return [strange_mass, 0.18, charm_mass]
    return [strange_mass, charm_mass]
