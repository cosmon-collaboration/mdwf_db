#!/usr/bin/env python3
"""
commands/glu_input.py

Generate GLU input files for gauge field utilities.
"""
import argparse, ast, sys, os
from pathlib import Path
from MDWFutils.db    import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.jobs.glu import generate_glu_input

def register(subparsers):
    p = subparsers.add_parser(
        'glu-input',
        help='Generate GLU input file for gauge field utilities',
        description="""
Generate a GLU input file for the GLU gauge field utility program.

WHAT THIS DOES:
• Creates a properly formatted GLU input file
• Uses ensemble parameters to set lattice dimensions
• Provides sensible defaults for all GLU parameters
• Allows customization of any parameter

GLU PROGRAM:
GLU is a gauge field utility program that can perform various operations
on lattice gauge configurations, including:
• Configuration smearing (STOUT, APE, etc.)
• Gauge fixing (Coulomb, Landau)
• Wilson loops and other gauge observables
• Configuration format conversions

PARAMETER CUSTOMIZATION:
GLU parameters use flat names (no dots) and can be overridden:

Common parameters (with defaults):
  CONFNO: 24              # Configuration number to process
  DIM_0, DIM_1, DIM_2: 16 # Spatial lattice dimensions (auto-set from ensemble)
  DIM_3: 48               # Temporal dimension (auto-set from ensemble)
  SMEARTYPE: STOUT        # Smearing algorithm
  SMITERS: 8              # Number of smearing iterations
  ALPHA1: 0.75            # Primary smearing parameter
  ALPHA2: 0.4             # Secondary smearing parameter
  ALPHA3: 0.2             # Tertiary smearing parameter
  GFTYPE: COULOMB         # Gauge fixing type
  GF_TUNE: 0.09           # Gauge fixing tuning parameter
  ACCURACY: 14            # Gauge fixing accuracy
  MAX_ITERS: 650          # Maximum gauge fixing iterations

CALCULATION TYPES:
  smearing:     Configuration smearing (default)
  gluon_props:  Gluon field measurements
  other:        Custom GLU operations

EXAMPLES:
  # Basic smearing input with defaults
  mdwf_db glu-input -e 1 -o smear.in

  # Custom smearing parameters
  mdwf_db glu-input -e 1 -o smear.in -g "CONFNO=168 SMITERS=50 ALPHA1=0.1"

  # Gauge fixing input
  mdwf_db glu-input -e 1 -o gauge_fix.in -t other -g "CONFNO=100 GFTYPE=LANDAU"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e','--ensemble', required=True,
                   help='Ensemble ID, directory path, or "." for current directory')
    p.add_argument('-o','--output-file', required=True,
                   help='Path where to write the GLU input file')
    p.add_argument('-g','--glu-params', default='',
                   help='Space-separated key=val pairs for GLU parameters. Example: "CONFNO=168 SMITERS=50 ALPHA1=0.1"')
    p.add_argument('-t','--type', default='smearing',
                   choices=['smearing', 'gluon_props', 'other'],
                   help='Type of GLU calculation (default: smearing)')
    p.set_defaults(func=do_glu_input)

def do_glu_input(args):
    # Resolve ensemble
    ensemble_id, ens = resolve_ensemble_identifier(args.db_file, args.ensemble)
    if ensemble_id is None:
        print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
        return 1

    # Parse GLU parameters
    glu_params = {}
    if args.glu_params:
        for pair in args.glu_params.split():
            if '=' not in pair:
                print(f"ERROR: Invalid parameter format '{pair}'. Use KEY=VALUE", file=sys.stderr)
                return 1
            key, value = pair.split('=', 1)
            glu_params[key] = value

    # Set lattice dimensions from ensemble parameters
    ens_params = ens['parameters']
    if 'L' in ens_params:
        glu_params.setdefault('DIM_0', str(ens_params['L']))
        glu_params.setdefault('DIM_1', str(ens_params['L']))
        glu_params.setdefault('DIM_2', str(ens_params['L']))
    if 'T' in ens_params:
        glu_params.setdefault('DIM_3', str(ens_params['T']))

    # Generate GLU input
    try:
        output_path = generate_glu_input(
            output_file=args.output_file,
            overrides=glu_params
        )
        return 0
    except Exception as e:
        print(f"ERROR: Failed to generate GLU input: {e}", file=sys.stderr)
        return 1