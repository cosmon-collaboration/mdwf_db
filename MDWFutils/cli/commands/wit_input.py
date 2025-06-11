import argparse, ast, sys
from MDWFutils.jobs.wit import generate_wit_input
from MDWFutils.db import get_ensemble_details

def register(subparsers):
    p = subparsers.add_parser(
        'wit-input',
        help='Generate a WIT input file for meson measurements',
        description="""
Generate a WIT input file for meson measurements. This command:
1. Creates a WIT input file with default parameters
2. Allows customization of parameters
3. Supports meson2pt calculations

Parameters can be set using dot notation:
  SECTION.KEY=value

Example:
  mdwf_db wit-input -e 1 -o DWF.in -w "MESON2PT.NPROP=4 MESON2PT.NCONF=100"
"""
    )
    p.add_argument(
        '--ensemble-id', '-e',
        dest='ensemble_id',
        type=int,
        required=True,
        help='ID of the ensemble to generate input for'
    )
    p.add_argument(
        '--output-file', '-o',
        dest='output_file',
        required=True,
        help='Path to output WIT input file'
    )
    p.add_argument(
        '--wit-params', '-w',
        dest='wit_params',
        default='',
        help='Space-separated key=val pairs for WIT parameters (e.g. "MESON2PT.NPROP=4")'
    )
    p.set_defaults(func=do_wit_input)

def do_wit_input(args):
    # Get ensemble details for validation
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1

    # Parse WIT parameters into nested dict
    wdict = {}
    for tok in args.wit_params.split():
        if '=' not in tok:
            print(f"ERROR: bad WIT-param {tok}", file=sys.stderr)
            return 1
        key, raw = tok.split('=',1)
        try:
            val = ast.literal_eval(raw)
        except:
            val = raw
        parts = key.split('.')
        d = wdict
        for p in parts[:-1]:
            d = d.setdefault(p,{})
        d[parts[-1]] = val

    # Add calculation type to parameters
    wdict.setdefault('Run name', {})['type'] = 'meson2pt'

    # Generate the input file
    generate_wit_input(args.output_file, custom_changes=wdict)
    return 0