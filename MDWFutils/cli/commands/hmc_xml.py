#!/usr/bin/env python3
"""
commands/hmc_xml.py

Sub‐command "hmc-xml": generate the HMCparameters XML (tepid, continue or reseed)
for a given ensemble.
"""
import sys
from pathlib import Path
import argparse

from MDWFutils.db import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.cli.ensemble_utils import migrate_ensemble_id_argument
from MDWFutils.jobs.hmc import generate_hmc_parameters

def register(subparsers: argparse._SubParsersAction):
    p = subparsers.add_parser(
        'hmc-xml',
        help='Generate HMC parameters XML file',
        description="""
Generate HMC parameters XML file for an ensemble. This command:
1. Creates or updates HMCparameters.xml with default parameters for the specified mode
2. Allows customization of any parameter
3. Supports different run modes

Available run modes:
- tepid: Initial thermalization run (TepidStart, no Metropolis test)
- continue: Continue from existing configuration (CheckpointStart, with Metropolis test)
- reseed: Start new run with different seed (CheckpointStartReseed, with Metropolis test)

Common XML parameters:
- StartTrajectory: Starting trajectory number
- Trajectories: Number of trajectories to generate
- MetropolisTest: Perform Metropolis test (true/false)
- StartingType: Start type (TepidStart/CheckpointStart/CheckpointStartReseed)
- Seed: Random seed (for reseed mode only)
- MDsteps: Number of MD steps
- trajL: Trajectory length

Example:
  mdwf_db hmc-xml -e 1 -m tepid -x "StartTrajectory=0 Trajectories=100"
"""
    )
    p.add_argument(
        '-e','--ensemble',
        required=True,
        help='Ensemble ID, directory path, or "." for current directory'
    )
    p.add_argument(
        '-m','--mode',
        choices=['tepid','continue','reseed'],
        required=True,
        help='Run mode: tepid (new), continue (existing), or reseed (new seed)'
    )
    p.add_argument(
        '-b','--base-dir',
        default='.',
        help='Root directory containing TUNING/ & ENSEMBLES/ (default: current directory)'
    )
    p.add_argument(
        '-x','--xml-params',
        required=True,
        help=('Space-separated key=val pairs to override XML defaults. Required: trajL, lvl_sizes. '
              'Example: "StartTrajectory=0 Trajectories=100 trajL=0.75 lvl_sizes=9,1,1"')
    )
    p.add_argument(
        '--out-dir',
        help='Optional output directory for HMCparameters.xml (defaults to the ensemble directory)'
    )
    p.set_defaults(func=do_hmc_xml)


def do_hmc_xml(args):
    # resolve flexible ensemble identifier
    ensemble_id, ens = resolve_ensemble_identifier(args.db_file, args.ensemble)
    if ensemble_id is None:
        print(f"ERROR: ensemble not found: {args.ensemble}", file=sys.stderr)
        return 1

    # resolve its on‐disk path
    ens_dir = Path(ens['directory']).resolve()
    target_dir = Path(args.out_dir).resolve() if getattr(args, 'out_dir', None) else ens_dir

    # parse the xml‐params string into a dict
    xdict = {}
    for tok in args.xml_params.split():
        if '=' not in tok:
            print(f"ERROR: bad xml param '{tok}'", file=sys.stderr)
            return 1
        k, v = tok.split('=', 1)
        xdict[k] = v
    
    # Check required XML parameters
    missing_params = []
    if 'trajL' not in xdict:
        missing_params.append('trajL')
    if 'lvl_sizes' not in xdict:
        missing_params.append('lvl_sizes')
    
    if missing_params:
        print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}", file=sys.stderr)
        return 1

    # generate the XML
    # This will produce HMCparameters.xml under target_dir
    generate_hmc_parameters(
        ensemble_dir = str(target_dir),
        mode         = args.mode,
        **xdict
    )

    print(f"Wrote HMCparameters.xml to {target_dir} (mode: {args.mode})")
    return 0