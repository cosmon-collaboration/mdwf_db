"""WIT input context builder and legacy helpers."""

from __future__ import annotations

import copy
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, MutableMapping, Optional

from MDWFutils.exceptions import ValidationError

from .utils import get_ensemble_doc, get_physics_params
from ..templates.loader import TemplateLoader
from ..templates.renderer import TemplateRenderer

_renderer = TemplateRenderer(TemplateLoader())


def _unflatten_params(flat_params: Dict) -> Dict:
    """Convert flat dotted keys to nested dictionaries, restoring legacy CLI behavior."""
    nested: Dict = {}
    for key, value in flat_params.items():
        if "." not in key:
            nested[key] = value
            continue
        parts = key.split(".")
        target = nested
        for part in parts[:-1]:
            existing = target.get(part)
            if not isinstance(existing, dict):
                existing = {}
                target[part] = existing
            target = existing
        target[parts[-1]] = value
    return nested


def build_wit_context(backend, ensemble_id: int, input_params: Dict) -> Dict:
    """Build template context for the WIT input command."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)
    unflattened = _unflatten_params(input_params or {})
    params = _build_parameters(physics, unflattened)
    sections = _ordered_dict_to_sections(params)
    
    # Add output directory info for standalone WIT input generation
    # Don't specify a subdirectory - let the user control via -o flag
    ensemble_dir = Path(ensemble["directory"]).resolve()
    return {
        "sections": sections,
        "_output_dir": str(ensemble_dir),
        "_output_prefix": "DWF",
    }


def render_wit_input(
    output_file: str,
    custom_params: Optional[Dict] = None,
    *,
    ensemble_params: Optional[Dict] = None,
    cli_format: bool = False,
    prune_prop_solvers: Optional[Iterable[int]] = None,
) -> str:
    """
    Render a WIT input file to disk (legacy helper used by job builders).
    """
    overrides = custom_params or {}
    if cli_format and overrides:
        overrides = convert_cli_to_wit_format(overrides)

    params = _build_parameters(ensemble_params or {}, overrides, prune_prop_solvers)
    sections = _ordered_dict_to_sections(params)
    content = _renderer.render("input/wit_input.j2", {"sections": sections})

    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


# --------------------------------------------------------------------------- #
# Parameter assembly helpers
# --------------------------------------------------------------------------- #

def _build_parameters(
    ensemble_params: Dict,
    overrides: Dict,
    prune_prop_solvers: Optional[Iterable[int]] = None,
) -> "OrderedDict[str, Dict[str, str]]":
    params = _default_params()
    if ensemble_params:
        _apply_ensemble_defaults(params, ensemble_params)
    if overrides:
        update_nested_dict(params, overrides)

    if prune_prop_solvers:
        witness = params.setdefault("Witness", {})
        prop_count, solver_count = prune_prop_solvers
        witness["no_prop"] = str(prop_count)
        witness["no_solver"] = str(solver_count)

    _finalize_params(params)
    return params


def _default_params() -> "OrderedDict[str, Dict[str, str]]":
    return OrderedDict(copy.deepcopy(_DEFAULT_WIT_PARAMS))


def _apply_ensemble_defaults(params: MutableMapping, ensemble_params: Dict) -> None:
    lattice = params.setdefault("Lattice parameters", {})
    for key in ("Ls", "b", "M5"):
        if key in ensemble_params:
            lattice[key] = str(ensemble_params[key])

    if "b" in lattice:
        try:
            lattice["c"] = str(float(lattice["b"]) - 1.0)
        except (TypeError, ValueError):
            pass
    elif "c" in ensemble_params:
        lattice["c"] = str(ensemble_params["c"])

    for mass_key, section in (("ml", "Propagator 0"), ("ms", "Propagator 1"), ("mc", "Propagator 2")):
        if mass_key in ensemble_params:
            try:
                mass = float(ensemble_params[mass_key])
                kappa = 1.0 / (2.0 * mass + 8.0)
                update_nested_dict(params.setdefault(section, {}), {"kappa": str(kappa)})
            except Exception:
                continue


def _finalize_params(params: MutableMapping) -> None:
    if "Lattice parameters" in params and "b" in params["Lattice parameters"]:
        try:
            b_value = float(params["Lattice parameters"]["b"])
            params["Lattice parameters"]["c"] = str(b_value - 1.0)
        except (TypeError, ValueError):
            pass

    witness = params.setdefault("Witness", {})
    no_prop = int(str(witness.get("no_prop", 3)))
    no_solver = int(str(witness.get("no_solver", 2)))
    no_prop = max(0, min(3, no_prop))
    no_solver = max(0, min(2, no_solver))

    for idx in range(no_prop, 3):
        params.pop(f"Propagator {idx}", None)

    for idx in range(no_solver, 2):
        params.pop(f"Solver {idx}", None)


def _ordered_dict_to_sections(params: "OrderedDict[str, Dict[str, str]]") -> List[Dict]:
    sections: List[Dict] = []
    for name, entries in params.items():
        ordered_entries = [
            {"key": key, "value": value}
            for key, value in entries.items()
        ]
        sections.append({"name": name, "entries": ordered_entries})
    return sections


# --------------------------------------------------------------------------- #
# Legacy helpers retained for compatibility
# --------------------------------------------------------------------------- #

def convert_cli_to_wit_format(cli_params: Dict[str, Dict]) -> Dict[str, Dict]:
    """Convert CLI-style keys (with underscores) to WIT section names."""
    wit_params: Dict[str, Dict] = {}
    for section_key, section_dict in cli_params.items():
        if section_key.endswith(("_0", "_1", "_2")):
            section_name = f"{section_key[:-2].replace('_', ' ')} {section_key[-1]}"
        else:
            section_name = section_key.replace("_", " ")

        converted = {}
        for param_key, param_value in section_dict.items():
            if param_key in ("pos", "mom", "twist") and isinstance(param_value, str):
                converted[param_key] = param_value.replace(",", " ")
            elif param_key in ("pos", "mom", "twist") and isinstance(param_value, (list, tuple)):
                converted[param_key] = " ".join(str(x) for x in param_value)
            else:
                converted[param_key] = param_value
        wit_params[section_name] = converted
    return wit_params


def update_nested_dict(target: MutableMapping, updates: Dict) -> MutableMapping:
    """Recursively merge updates into target."""
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            update_nested_dict(target[key], value)
        else:
            target[key] = value
    return target


# --------------------------------------------------------------------------- #
# Default parameter definitions (ordered)
# --------------------------------------------------------------------------- #

_DEFAULT_WIT_PARAMS = [
    (
        "Run name",
        {"name": "ck"},
    ),
    (
        "Directories",
        {"cnfg_dir": "../cnfg_STOUT8/"},
    ),
    (
        "Configurations",
        {"first": "CFGNO", "last": "CFGNO", "step": "4"},
    ),
    (
        "Random number generator",
        {"level": "0", "seed": "3993"},
    ),
    (
        "Lattice parameters",
        {"Ls": "10", "M5": "1.0", "b": "1.75", "c": "0.75"},
    ),
    (
        "Boundary conditions",
        {"type": "APeri"},
    ),
    (
        "Witness",
        {"no_prop": "3", "no_solver": "2"},
    ),
    (
        "Solver 0",
        {
            "solver": "CG",
            "nkv": "24",
            "isolv": "1",
            "nmr": "3",
            "ncy": "3",
            "nmx": "8000",
            "exact_deflation": "true",
        },
    ),
    (
        "Solver 1",
        {
            "solver": "CG",
            "nkv": "24",
            "isolv": "1",
            "nmr": "3",
            "ncy": "3",
            "nmx": "8000",
            "exact_deflation": "false",
        },
    ),
    (
        "Exact Deflation",
        {
            "Cheby_fine": "0.01,-1,24",
            "Cheby_smooth": "0,0,0",
            "Cheby_coarse": "0,0,0",
            "kappa": "0.125",
            "res": "1E-5",
            "nmx": "64",
            "Ns": "64",
        },
    ),
    (
        "Propagator 0",
        {
            "Noise": "Z2xZ2",
            "Source": "Wall",
            "Dilution": "PS",
            "pos": "0 0 0 -1",
            "mom": "0 0 0 0",
            "twist": "0 0 0",
            "kappa": "KAPPA_L",
            "mu": "0.",
            "Seed": "54321",
            "idx_solver": "0",
            "res": "1E-12",
            "sloppy_res": "1E-4",
        },
    ),
    (
        "Propagator 1",
        {
            "Noise": "Z2xZ2",
            "Source": "Wall",
            "Dilution": "PS",
            "pos": "0 0 0 -1",
            "mom": "0 0 0 0",
            "twist": "0 0 0",
            "kappa": "KAPPA_S",
            "mu": "0.",
            "Seed": "54321",
            "idx_solver": "1",
            "res": "1E-12",
            "sloppy_res": "1E-6",
        },
    ),
    (
        "Propagator 2",
        {
            "Noise": "Z2xZ2",
            "Source": "Wall",
            "Dilution": "PS",
            "pos": "0 0 0 -1",
            "mom": "0 0 0 0",
            "twist": "0 0 0",
            "kappa": "KAPPA_C",
            "mu": "0.",
            "Seed": "54321",
            "idx_solver": "1",
            "res": "5E-15",
            "sloppy_res": "5E-15",
        },
    ),
    (
        "AMA",
        {
            "NEXACT": "2",
            "SLOPPY_PREC": "1E-5",
            "NHITS": "1",
            "NT": "48",
        },
    ),
]


__all__ = [
    "build_wit_context",
    "render_wit_input",
    "convert_cli_to_wit_format",
    "update_nested_dict",
]


# Backward compatibility
def generate_wit_input(*args, **kwargs):
    """Alias for legacy callers."""
    return render_wit_input(*args, **kwargs)

