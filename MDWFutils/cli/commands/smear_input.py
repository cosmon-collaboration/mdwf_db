#!/usr/bin/env python3
"""
commands/smear_input.py

Sub‐command “smear-input”: just build the GLU input file to smear configs.
"""
import sys
from pathlib import Path
import argparse

from MDWFutils.db            import get_ensemble_details
from MDWFutils.jobs.glu import generate_glu_input


def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(
        'smear-input',
        help='Generate GLU input file to smear an ensemble'
    )
    p.add_argument(
        '-e','--ensemble-id',
        type=int,
        required=True,
        help='ID of the ensemble in the DB'
    )
    p.add_argument(
        '-b','--base-dir',
        default='.',
        help='Root of TUNING/ & ENSEMBLES/ (default: CWD)'
    )
    p.add_argument(
        '-s','--smear-params',
        default='',
        help='Space‐separated key=val to override defaults (e.g. SMITERS=10)'
    )
    p.set_defaults(func=do_smear_input)


def do_smear_input(args):
    # Lookup ensemble
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found",
              file=sys.stderr)
        return 1

    # Resolve on‐disk directory
    raw = Path(ens['directory'])
    if raw.is_absolute():
        ens_dir = raw
    else:
        ens_dir = (Path(args.base_dir) / raw).resolve()

    # Pull L, T from the DB‐params
    params = ens['parameters']
    missing = [k for k in ("L","T") if k not in params]
    if missing:
        print(f"ERROR: ensemble missing required dims: {missing}",
              file=sys.stderr)
        return 1

    L = params["L"]
    T = params["T"]
    dims = {
        "DIM_0": L,
        "DIM_1": L,
        "DIM_2": L,
        "DIM_3": T
    }

    # Parse any CLI overrides
    cli = {}
    for tok in args.smear_params.split():
        if "=" not in tok:
            print(f"ERROR: bad smear-param '{tok}'", file=sys.stderr)
            return 1
        k, v = tok.split("=", 1)
        cli[k] = v

    # Merge dims + CLI (CLI wins)
    overrides = dims.copy()
    overrides.update(cli)

    outpath = Path(ens_dir) / "smear.in"
    
    # Generate the GLU input
    outfile = generate_glu_input(str(outpath), overrides)
    print(f"Wrote smear‐input file: {outpath}")
    return 0