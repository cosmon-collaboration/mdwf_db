"""Context builder for GLU smearing input files."""

from __future__ import annotations

from typing import Dict

from MDWFutils.exceptions import ValidationError

from .utils import ensure_keys, get_ensemble_doc, get_physics_params

DEFAULT_GLU_PARAMS = {
    "MODE": "SMEARING",
    "CONFNO": "24",
    "RANDOM_TRANSFORM": "NO",
    "SEED": "0",
    "CUTTYPE": "GLUON_PROPS",
    "HEADER": "NERSC",
    "DIM_0": "16",
    "DIM_1": "16",
    "DIM_2": "16",
    "DIM_3": "48",
    "GFTYPE": "COULOMB",
    "GF_TUNE": "0.09",
    "ACCURACY": "14",
    "MAX_ITERS": "650",
    "FIELD_DEFINITION": "LINEAR",
    "MOM_CUT": "CYLINDER_CUT",
    "MAX_T": "7",
    "MAXMOM": "4",
    "CYL_WIDTH": "2.0",
    "ANGLE": "60",
    "OUTPUT": "./",
    "SMEARTYPE": "STOUT",
    "DIRECTION": "ALL",
    "SMITERS": "8",
    "ALPHA1": "0.75",
    "ALPHA2": "0.4",
    "ALPHA3": "0.2",
    "U1_MEAS": "U1_RECTANGLE",
    "U1_ALPHA": "0.07957753876221914",
    "U1_CHARGE": "-1.0",
    "CONFIG_INFO": "2+1DWF_b2.25_TEST",
    "STORAGE": "CERN",
    "BETA": "6.0",
    "ITERS": "1500",
    "MEASURE": "1",
    "OVER_ITERS": "4",
    "SAVE": "25",
    "THERM": "100",
}


def build_glu_context(backend, ensemble_id: int, input_params: Dict) -> Dict:
    """Build the context required for the GLU input template."""
    ensemble = get_ensemble_doc(backend, ensemble_id)
    physics = get_physics_params(ensemble)
    ensure_keys(physics, ["L", "T"])

    context = DEFAULT_GLU_PARAMS.copy()
    context.update(
        {
            "DIM_0": str(physics["L"]),
            "DIM_1": str(physics["L"]),
            "DIM_2": str(physics["L"]),
            "DIM_3": str(physics["T"]),
        }
    )

    for key, value in (input_params or {}).items():
        _apply_override(context, key, value)

    return context


def _apply_override(context: Dict, key: str, value) -> None:
    """Support both flat and dotted overrides."""
    if value is None:
        return
    if "." in key:
        _, child = key.split(".", 1)
        target_key = child
    else:
        target_key = key

    if target_key not in context:
        raise ValidationError(f"Unknown GLU parameter '{target_key}'")

    context[target_key] = str(value)


__all__ = ["build_glu_context"]

