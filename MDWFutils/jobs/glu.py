"""Context builder for GLU smearing input files."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from MDWFutils.exceptions import ValidationError
from MDWFutils.templates.loader import TemplateLoader
from MDWFutils.templates.renderer import TemplateRenderer

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

    # Add output directory info for standalone GLU input generation
    ensemble_dir = Path(ensemble["directory"]).resolve()
    context["_output_dir"] = str(ensemble_dir)
    context["_output_prefix"] = "glu_smear"

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


def generate_glu_input(output_file: str, overrides: Optional[Dict] = None) -> str:
    """
    Generate GLU input file using template with defaults and overrides.
    
    This function provides backward compatibility with the old API where
    smear.py and wflow.py directly call generate_glu_input().
    
    Args:
        output_file: Path to output file
        overrides: Dictionary of parameter overrides using flat parameter names
                  e.g. {'DIM_0': '32', 'CONFNO': '100', 'SMITERS': '10', 'ALPHA1': '0.8'}
    
    Returns:
        Path to generated file
    """
    if overrides is None:
        overrides = {}
    
    # Start with defaults and apply overrides
    context = DEFAULT_GLU_PARAMS.copy()
    for key, value in overrides.items():
        _apply_override(context, key, value)
    
    # Render using template system
    loader = TemplateLoader()
    renderer = TemplateRenderer(loader)
    content = renderer.render("input/glu_input.j2", context)
    
    # Write the file
    outf = Path(output_file)
    outf.parent.mkdir(parents=True, exist_ok=True)
    outf.write_text(content)
    
    return str(outf)


__all__ = ["build_glu_context", "generate_glu_input"]

