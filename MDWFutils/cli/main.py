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

def get_default_db_connection():
    """
    Get default database connection string, preferring MongoDB URL over SQLite file.
    
    Returns:
        str: MongoDB connection string if MDWF_DB_URL is set, otherwise SQLite file path
    """
    # Prefer MongoDB URL if set
    mongo_url = os.getenv('MDWF_DB_URL')
    if mongo_url:
        return mongo_url
    
    # Fall back to SQLite file discovery
    sqlite_file = os.getenv('MDWF_DB', find_database_file())
    return sqlite_file

def _generate_command_help(subparsers):
    """Auto-generate command list from registered subparsers."""
    commands = []
    
    for name in sorted(subparsers.choices.keys()):
        parser = subparsers.choices[name]
        help_text = parser.description or ''
        commands.append(f"  {name:<20} {help_text}")
    
    lines = ["Available commands:", ""]
    lines.extend(commands)
    return "\n".join(lines)

def main():
    # Create parser WITHOUT epilog initially
    parser = argparse.ArgumentParser(
        prog="mdwf_db",
        description="MDWF Database Management Tool for Domain Wall Fermion Lattice QCD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subs   = parser.add_subparsers(dest='cmd')

    # Dynamically import every module in cli/commands and call its register()
    pkg = importlib.import_module('MDWFutils.cli.commands')
    for finder, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"MDWFutils.cli.commands.{name}")
        # each module must define register(subparsers)
        if hasattr(mod, 'register'):
            mod.register(subs)

    # NOW generate and set epilog with registered commands
    command_help = _generate_command_help(subs)
    parser.epilog = f"""
{command_help}

FLEXIBLE IDENTIFIERS:
Most commands accept either ensemble IDs (integers) or directory paths:
  -e 1                    # Use ensemble ID
  -e ./TUNING/b6.0/...    # Use relative path
  -e /full/path/to/ens    # Use absolute path
  -e .                    # Use current directory (when inside ensemble)

DATABASE CONNECTION:
Use MDWF_DB_URL environment variable:
  export MDWF_DB_URL=mongodb://host:port/database?authSource=admin

For detailed help: mdwf_db <command> --help
"""

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return 1

    # Validate DB presence for commands that require an existing DB
    # Allow init-db to create a new database
    if args.cmd != 'init-db':
        db_conn = get_default_db_connection()
        if not db_conn:
            print("ERROR: No database connection configured.")
            print("Hint: Set MDWF_DB_URL environment variable.")
                return 1

    # every module must set args.func to its handler in register()
    return args.func(args)

if __name__=='__main__':
    sys.exit(main())