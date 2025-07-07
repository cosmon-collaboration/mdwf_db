import os
from pathlib import Path
from MDWFutils.db import get_ensemble_details

def generate_glu_input(
    output_file: str,
    overrides: dict = None
) -> str:
    """
    Generate GLU input file using f-string template with defaults.
    
    Args:
        output_file: Path to output file
        overrides: Dictionary of parameter overrides using flat parameter names
                  e.g. {'DIM_0': '32', 'CONFNO': '100', 'SMITERS': '10', 'ALPHA1': '0.8'}
                  Also supports legacy dot notation for backward compatibility
    
    Returns:
        Path to generated file
    """
    if overrides is None:
        overrides = {}
    
    # Default values - using flat parameter names since all are unique
    defaults = {
        # Top-level parameters
        'MODE': 'SMEARING',
        'CONFNO': '24',
        'RANDOM_TRANSFORM': 'NO',
        'SEED': '0',
        'CUTTYPE': 'GLUON_PROPS',
        
        # HEADER section parameters
        'HEADER': 'NERSC',
        'DIM_0': '16',
        'DIM_1': '16', 
        'DIM_2': '16',
        'DIM_3': '48',
        
        # GFTYPE section parameters
        'GFTYPE': 'COULOMB',
        'GF_TUNE': '0.09',
        'ACCURACY': '14',
        'MAX_ITERS': '650',
        
        # FIELD_DEFINITION section parameters
        'FIELD_DEFINITION': 'LINEAR',
        'MOM_CUT': 'CYLINDER_CUT',
        'MAX_T': '7',
        'MAXMOM': '4',
        'CYL_WIDTH': '2.0',
        'ANGLE': '60',
        'OUTPUT': './',
        
        # SMEARTYPE section parameters
        'SMEARTYPE': 'STOUT',
        'DIRECTION': 'ALL',
        'SMITERS': '8',
        'ALPHA1': '0.75',
        'ALPHA2': '0.4',
        'ALPHA3': '0.2',
        
        # U1_MEAS section parameters
        'U1_MEAS': 'U1_RECTANGLE',
        'U1_ALPHA': '0.07957753876221914',
        'U1_CHARGE': '-1.0',
        
        # CONFIG_INFO section parameters
        'CONFIG_INFO': '2+1DWF_b2.25_TEST',
        'STORAGE': 'CERN',
        
        # BETA section parameters
        'BETA': '6.0',
        'ITERS': '1500',
        'MEASURE': '1',
        'OVER_ITERS': '4',
        'SAVE': '25',
        'THERM': '100'
    }
    
    # Apply overrides - support both flat names and legacy dot notation
    params = defaults.copy()
    for key, value in overrides.items():
        if '.' in key:
            # Legacy dot notation support - convert to flat name
            parent, child = key.split('.', 1)
            if child in defaults:
                params[child] = str(value)
            else:
                print(f"Warning: Unknown parameter '{child}' in '{key}'")
        else:
            # Flat parameter name (preferred)
            if key in defaults:
                params[key] = str(value)
            else:
                print(f"Warning: Unknown parameter '{key}'")
    
    # Generate content using f-string template
    content = f"""MODE = {params['MODE']}    
HEADER = {params['HEADER']}
    DIM_0 = {params['DIM_0']}
    DIM_1 = {params['DIM_1']}
    DIM_2 = {params['DIM_2']}
    DIM_3 = {params['DIM_3']}
CONFNO = {params['CONFNO']}
RANDOM_TRANSFORM = {params['RANDOM_TRANSFORM']}
SEED = {params['SEED']}
GFTYPE = {params['GFTYPE']}
    GF_TUNE = {params['GF_TUNE']}
    ACCURACY = {params['ACCURACY']}
    MAX_ITERS = {params['MAX_ITERS']}
CUTTYPE = {params['CUTTYPE']}
FIELD_DEFINITION = {params['FIELD_DEFINITION']}
    MOM_CUT = {params['MOM_CUT']}
    MAX_T = {params['MAX_T']}
    MAXMOM = {params['MAXMOM']}
    CYL_WIDTH = {params['CYL_WIDTH']}
    ANGLE = {params['ANGLE']}
    OUTPUT = {params['OUTPUT']}
SMEARTYPE = {params['SMEARTYPE']}
    DIRECTION = {params['DIRECTION']}
    SMITERS = {params['SMITERS']}
    ALPHA1 = {params['ALPHA1']}
    ALPHA2 = {params['ALPHA2']}
    ALPHA3 = {params['ALPHA3']}
U1_MEAS = {params['U1_MEAS']}
    U1_ALPHA = {params['U1_ALPHA']}
    U1_CHARGE = {params['U1_CHARGE']}
CONFIG_INFO = {params['CONFIG_INFO']}
    STORAGE = {params['STORAGE']}
BETA = {params['BETA']}
    ITERS = {params['ITERS']}
    MEASURE = {params['MEASURE']}
    OVER_ITERS = {params['OVER_ITERS']}
    SAVE = {params['SAVE']}
    THERM = {params['THERM']}
"""
    
    # Write the file
    outf = Path(output_file)
    outf.parent.mkdir(parents=True, exist_ok=True)
    
    with outf.open("w") as f:
        f.write(content)
    
    print(f"Generated GLU input file: {outf}")
    return str(outf)

def update_glu_parameter(content: str, key: str, value: str) -> str:
    """
    Update a single parameter in GLU input content by regenerating with override.
    This is a convenience function for backward compatibility.
    """
    # For simplicity, we could parse the existing content and regenerate
    # But since this is mainly used for testing, we'll keep it simple
    lines = content.split('\n')
    
    if '.' in key:
        # Handle nested parameters
        parent, child = key.split('.', 1)
        in_section = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f'{parent} ='):
                in_section = True
                continue
            elif in_section:
                if line.strip() and not line.startswith('    '):
                    break
                elif line.strip().startswith(f'{child} ='):
                    indent = line[:len(line) - len(line.lstrip())]
                    lines[i] = f'{indent}{child} = {value}'
                    break
    else:
        # Handle flat parameter names (preferred)
        for i, line in enumerate(lines):
            if line.strip().startswith(f'{key} ='):
                indent = line[:len(line) - len(line.lstrip())]
                lines[i] = f'{indent}{key} = {value}'
                break
    
    return '\n'.join(lines)

def get_glu_parameter(content: str, key: str) -> str:
    """
    Get a parameter value from GLU input content.
    """
    lines = content.split('\n')
    
    if '.' in key:
        # Handle nested parameters
        parent, child = key.split('.', 1)
        in_section = False
        for line in lines:
            if line.strip().startswith(f'{parent} ='):
                in_section = True
                continue
            elif in_section:
                if line.strip() and not line.startswith('    '):
                    break
                elif line.strip().startswith(f'{child} ='):
                    return line.split('=', 1)[1].strip()
    else:
        # Handle flat parameter names (preferred)
        for line in lines:
            if line.strip().startswith(f'{key} ='):
                return line.split('=', 1)[1].strip()
    
    return None