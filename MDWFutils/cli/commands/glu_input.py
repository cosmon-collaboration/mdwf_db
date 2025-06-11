import argparse, ast, sys, os
from MDWFutils.db    import get_ensemble_details
from MDWFutils.jobs.glu import generate_glu_input

def register(subparsers):
    p = subparsers.add_parser('glu-input', 
        help='Generate GLU input file',
        description="""
Generate a GLU input file for smearing or gluon measurements. This command:
1. Creates a GLU input file with default parameters
2. Allows customization of any parameter
3. Supports different calculation types

Parameters can be specified in sections using dot notation:
  SECTION.KEY=value

Example:
  Configurations.first=168 Configurations.last=372
  Smearing.steps=50
  Smearing.alpha=0.1

Available calculation types:
- smearing: Configuration smearing (default)
- gluon_props: Gluon propagator measurements
- other: Custom measurements
"""
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble to generate input for')
    p.add_argument('-o','--output-file', required=True,
                   help='Path where to write the GLU input file')
    p.add_argument('-g','--glu-params', default='',
                   help=('Space-separated key=val pairs for GLU input. '
                         'Use dot notation for sections: SECTION.KEY=val'))
    p.add_argument('-t','--type', default='smearing',
                   choices=['smearing', 'gluon_props', 'other'],
                   help='Type of GLU calculation (default: smearing)')
    p.set_defaults(func=do_glu_input)

def do_glu_input(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: Ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1
    ens_dir = ens['directory']

    # Determine output file path
    out_file = args.output_file
    if not os.path.isabs(out_file):
        out_file = os.path.join(ens_dir, out_file)

    # Parse GLU parameters
    gdict = {}
    glu_params = args.glu_params
    if isinstance(glu_params, str):
        glu_params = glu_params.split()
    elif glu_params is None:
        glu_params = []
    for tok in glu_params:
        if '=' not in tok:
            print(f"ERROR: bad GLU param '{tok}'", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        gdict[k] = v

    # Only pass output_file and gdict (as overrides)
    generate_glu_input(out_file, gdict)
    return 0 