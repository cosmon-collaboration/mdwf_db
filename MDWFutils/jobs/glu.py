import os
import copy
from pathlib import Path
from MDWFutils.db import get_ensemble_details

DEFAULT_PARAMS = {
    "MODE": "SMEARING",    
    "HEADER": {
        "value": "NERSC",
        "DIM_0": "16",
        "DIM_1": "16",
        "DIM_2": "16",
        "DIM_3": "48"
    },
    "CONFNO": "0",
    "RANDOM_TRANSFORM": "NO",
    "SEED": "0",
    "GFTYPE": {
        "value": "COULOMB",
        "GF_TUNE": "0.09",
        "ACCURACY": "14",
        "MAX_ITERS": "650"
    },
    "CUTTYPE": "GLUON_PROPS",
    "FIELD_DEFINITION": {
        "value": "LINEAR",
        "MOM_CUT": "CYLINDER_CUT",
        "MAX_T": "7",
        "MAXMOM": "4",
        "CYL_WIDTH": "2.0",
        "ANGLE": "60",
        "OUTPUT": "./"
    },
    "SMEARTYPE": {
        "value": "STOUT",
        "DIRECTION": "ALL",
        "SMITERS": "8",
        "ALPHA1": "0.75",
        "ALPHA2": "0.4",
        "ALPHA3": "0.2"
    },
    "U1_MEAS": {
        "value": "U1_RECTANGLE",
        "U1_ALPHA": "0.07957753876221914",
        "U1_CHARGE": "-1.0"
    },
    "CONFIG_INFO": {
        "value": "2+1DWF_b2.25_TEST",
        "STORAGE": "CERN"
    },
    "BETA": {
        "value": "6.0",
        "ITERS": "1500",
        "MEASURE": "1",
        "OVER_ITERS": "4",
        "SAVE": "25",
        "THERM": "100"
    }
}

def update_nested_dict(d, updates):
    """
    Recursively merge updates into d.
    """
    for key, val in updates.items():
        if isinstance(val, dict) and key in d and isinstance(d[key], dict):
            update_nested_dict(d[key], val)
        else:
            d[key] = val
    return d

def generate_glu_input(
    output_file: str,
    overrides: dict = None
) -> str:
    """
    Write out the GLU input file merging DEFAULT_PARAMS with flat overrides.
    All keys (top-level + nested) must be unique.  

    Example:
      generate_glu_input(
        "out/glu.in",
        {"ALPHA1": "0.8", "ITERS": "100"}
      )
    """
    # Start from a fresh copy of the defaults
    params = copy.deepcopy(DEFAULT_PARAMS)
    
    # Apply overrides
    if overrides:
        update_nested_dict(params, overrides)

    # Ensure output directory exists
    outf = Path(output_file)
    outf.parent.mkdir(parents=True, exist_ok=True)

    # Dump the parameters
    with outf.open("w") as f:
        for key, val in params.items():
            if isinstance(val, dict):
                # Write section value first
                if 'value' in val:
                    f.write(f"{key} = {val['value']}\n")
                # Then write remaining parameters
                for subk, subv in val.items():
                    if subk == 'value':
                        continue
                    f.write(f"    {subk} = {subv}\n")
            else:
                f.write(f"{key} = {val}\n")

    print(f"Generated GLU input file: {outf}")
    return str(outf)