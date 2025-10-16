#!/usr/bin/env python3
import argparse
import pkgutil
import importlib
import os
import sys
from pathlib import Path

def find_database_file():
    """
    Find the mdwf_ensembles.db file by walking up the directory tree.
    
    Returns:
        str: Path to the database file, or default path if not found
    """
    current_dir = Path.cwd()
    db_filename = 'mdwf_ensembles.db'
    
    # Walk up the directory tree
    for parent in [current_dir] + list(current_dir.parents):
        db_path = parent / db_filename
        if db_path.exists():
            return str(db_path)
    
    # If not found, return default path in current directory
    return str(current_dir / db_filename)

def main():
    parser = argparse.ArgumentParser(
        prog="mdwf_db",
        description="MDWF Database Management Tool for Domain Wall Fermion Lattice QCD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available commands by category:

DATABASE MANAGEMENT:
  init-db             Initialize database and directory structure
  add-ensemble        Add ensemble to database (supports pre-existing directories)
  remove-ensemble     Remove ensemble and all its operations from database
  promote-ensemble    Move ensemble from TUNING to PRODUCTION status
  query              List ensembles or show detailed info for one ensemble
  clear-history      Clear operation history while preserving ensemble record
  nickname           Set or clear a human-friendly nickname for an ensemble

JOB SCRIPT GENERATION:
  hmc-script         Generate HMC XML and SLURM script for gauge generation
  hmc-xml            Generate standalone HMC XML parameter file
  smear-script       Generate GLU smearing SLURM script
  wflow-script       Generate gradient flow SLURM script
  meson2pt-script    Generate WIT meson correlator measurement script
  mres-script        Generate WIT mres measurement script
  zv-script          Generate Zv correlator measurement script
  glu-input          Generate GLU input file for gauge field utilities
  wit-input          Generate WIT input file for correlator measurements

OPERATION TRACKING:
  update             Record or update operation status and parameters

CONFIGURATION MANAGEMENT:
  default_params     Manage ensemble default parameter files for operation parameters
  scan               Scan cnfg/ folders to store config ranges and optionally scan filesystem

FLEXIBLE IDENTIFIERS:
Most commands accept either ensemble IDs (integers) or directory paths:
  -e 1                    # Use ensemble ID
  -e ./TUNING/b6.0/...    # Use relative path
  -e /full/path/to/ens    # Use absolute path
  -e .                    # Use current directory (when inside ensemble)

DATABASE AUTO-DISCOVERY:
The database file is automatically found by walking up the directory tree.
No need to specify --db-file when working within project directories.

For detailed help: mdwf_db <command> --help
"""
    )

    # Use environment variable if set, otherwise search up directory tree
    DEFAULT_DB = os.getenv('MDWF_DB', find_database_file())
    
    db_parent = argparse.ArgumentParser(add_help=False)
    db_parent.add_argument(
        '--db-file',
        default=DEFAULT_DB,
        help='Path to the SQLite DB (or set MDWF_DB env). Auto-discovered by walking up directory tree.'
    )

    subs   = parser.add_subparsers(dest='cmd')

    orig_add = subs.add_parser
    def add_parser(name, **kwargs):
        # collect any existing parents, make them into a list
        parents = kwargs.get('parents', [])
        if not isinstance(parents, list):
            parents = [parents]
        # ensure our db_parent is always there
        parents.append(db_parent)
        kwargs['parents'] = parents
        return orig_add(name, **kwargs)
    
    subs.add_parser = add_parser

    # Dynamically import every module in cli/commands and call its register()
    pkg = importlib.import_module('MDWFutils.cli.commands')
    for finder, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"MDWFutils.cli.commands.{name}")
        # each module must define register(subparsers)
        if hasattr(mod, 'register'):
            mod.register(subs)

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return 1

    # Validate DB presence for commands that require an existing DB
    # Allow init-db to create a new database
    if args.cmd != 'init-db':
        db_path = Path(getattr(args, 'db_file', find_database_file()))
        if not db_path.exists():
            print("ERROR: No database file found.")
            print("Hint: Run from your mdwf project directory (where mdwf_ensembles.db lives) or pass --db-file.")
            return 1

    # every module must set args.func to its handler in register()
    return args.func(args)

if __name__=='__main__':
    sys.exit(main())