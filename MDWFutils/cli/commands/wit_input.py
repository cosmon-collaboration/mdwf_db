#!/usr/bin/env python3
"""
commands/wit_input.py

Generate WIT input files for meson correlator measurements.
"""
import argparse, ast, sys, os
from pathlib import Path
from MDWFutils.db import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.jobs.wit import generate_wit_input

def register(subparsers):
    p = subparsers.add_parser(
        'wit-input', 
        help='Generate WIT input file for correlator measurements',
        description="""
Generate a WIT input file (DWF.in) for meson correlator measurements.

WHAT THIS DOES:
• Creates a properly formatted WIT input file
• Uses ensemble parameters to set lattice and physics parameters
• Provides sensible defaults for all WIT parameters
• Allows customization of any parameter

WIT PROGRAM:
WIT (Wilson Improved Twisted mass) computes meson 2-point correlators
from Domain Wall Fermion propagators on gauge configurations.

PARAMETER CUSTOMIZATION:
WIT parameters use dot notation (SECTION.KEY=value) and can be overridden:

Run configuration:
  name: u_stout8                    # Run name
  cnfg_dir: ../cnfg_stout8/         # Configuration directory

Configuration range:
  Configurations.first: 0           # First configuration number
  Configurations.last: 100          # Last configuration number  
  Configurations.step: 4            # Step between configurations

Lattice parameters (auto-set from ensemble):
  Ls: (from ensemble)               # Domain wall extent
  M5: 1.0                          # Domain wall mass
  b: (from ensemble)                # Domain wall height
  c: (automatically b-1)           # Domain wall parameter (calculated as b-1)

Measurement setup:
  Witness.no_prop: 3                # Number of propagators (light, strange, charm)
  Witness.no_solver: 2              # Number of solvers

Solver parameters:
  Solver_0.solver: CG               # Conjugate gradient solver
  Solver_0.nmx: 8000                # Maximum iterations
  Solver_0.exact_deflation: true    # Use exact deflation
  Solver_1.exact_deflation: false   # Second solver settings

Propagator parameters:
  Propagator_0.Source: Wall         # Source type (Wall, Point)
  Propagator_0.Dilution: PS         # Dilution scheme
  Propagator_0.pos: 0,0,0,-1        # Source position (comma-separated)
  Propagator_0.mom: 0,0,0,0         # Momentum (comma-separated)
  Propagator_0.twist: 0,0,0         # Twist angles (comma-separated)
  Propagator.Seed: 54321            # Propagator seed (REQUIRED, applies to ALL propagators)
  Propagator_0.kappa: (auto from ml) # Light quark kappa
  Propagator_1.kappa: (auto from ms) # Strange quark kappa
  Propagator_2.kappa: (auto from mc) # Charm quark kappa

Random number generator:
  Random_number_generator.seed: 3993 # RNG seed (optional, different from propagator seed)

Note: Kappa values are automatically calculated from quark masses
stored in the ensemble parameters. Do not set these manually.

EXAMPLES:
  # Basic WIT input with defaults
  mdwf_db wit-input -e 1 -o DWF.in

  # Custom configuration range
  mdwf_db wit-input -e 1 -o DWF.in -w "Configurations.first=100 Configurations.last=200"

  # Point source measurement
  mdwf_db wit-input -e 1 -o DWF.in -w "Propagator_0.Source=Point Propagator_0.pos=0,0,0,0"

  # Custom solver settings
  mdwf_db wit-input -e 1 -o DWF.in -w "Solver_0.nmx=10000 Solver_0.res=1E-12"

  # Custom propagator seed (applies to all propagators)
  mdwf_db wit-input -e 1 -o DWF.in -w "Propagator.Seed=98765"

For complete parameter documentation, see the WIT manual.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Flexible identifier (preferred)
    p.add_argument('--ensemble', '-e', required=False,
                   help='Ensemble ID, directory path, or "." for current directory')
    # Legacy integer-only option for backward compatibility
    p.add_argument('--ensemble-id', dest='ensemble_id', type=int, required=False,
                   help='[DEPRECATED] Ensemble ID (use -e/--ensemble for flexible ID or path)')
    p.add_argument('--output-file', '-o', required=True,
                   help='Path to output WIT input file (e.g., DWF.in)')
    p.add_argument('--wit-params', '-w', default='',
                   help='Space-separated key=val pairs for WIT parameters using dot notation. Example: "Configurations.first=100 Configurations.last=200"')
    p.set_defaults(func=do_wit_input)

def do_wit_input(args):
    # Resolve ensemble from flexible identifier first, then legacy --ensemble-id
    ensemble_id = None
    ens = None
    if getattr(args, 'ensemble', None):
        eid, info = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if eid is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
            return 1
        ensemble_id, ens = eid, info
    elif getattr(args, 'ensemble_id', None) is not None:
        ens = get_ensemble_details(args.db_file, args.ensemble_id)
        if not ens:
            print(f"ERROR: Ensemble {args.ensemble_id} not found", file=sys.stderr)
            return 1
        ensemble_id = args.ensemble_id
    else:
        print("ERROR: Missing ensemble identifier. Use -e/--ensemble (ID or path) or --ensemble-id.", file=sys.stderr)
        return 1

    # Parse WIT parameters into nested dict
    wdict = {}
    if args.wit_params:
        for tok in args.wit_params.split():
            if '=' not in tok:
                print(f"ERROR: Invalid parameter format '{tok}'. Use SECTION.KEY=VALUE", file=sys.stderr)
                return 1
            key, raw = tok.split('=', 1)
            try:
                val = ast.literal_eval(raw)
            except:
                val = raw
            
            # Build nested dictionary structure
            parts = key.split('.')
            d = wdict
            for p in parts[:-1]:
                if p not in d or not isinstance(d[p], dict):
                    d[p] = {}
                d = d[p]
            d[parts[-1]] = val

    # Generate WIT input
    try:
        output_path = Path(args.output_file)
        generate_wit_input(
            ensemble_params=ens['parameters'],
            output_file=output_path,
            custom_params=wdict,
            cli_format=True
        )
        print(f"Generated WIT input file: {output_path.resolve()}")
        return 0
        
    except Exception as e:
        print(f"ERROR: Failed to generate WIT input: {e}", file=sys.stderr)
        return 1