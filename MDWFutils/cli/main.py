#!/usr/bin/env python3
import argparse
import pkgutil
import importlib
import sys
from .runtime import get_default_db_connection

def main():
    # Create parser WITHOUT epilog initially
    parser = argparse.ArgumentParser(
        prog="mdwf_db",
        description="MDWF Database Management Tool for Domain Wall Fermion Lattice QCD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Use metavar='<command>' to suppress verbose {cmd1,cmd2,...} display
    subs = parser.add_subparsers(dest='cmd', metavar='<command>')

    # Dynamically import every module in cli/commands and call its register()
    pkg = importlib.import_module('MDWFutils.cli.commands')
    for finder, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"MDWFutils.cli.commands.{name}")
        # each module must define register(subparsers)
        if hasattr(mod, 'register'):
            mod.register(subs)

    # Set epilog with usage hints (command list is shown by argparse automatically)
    parser.epilog = """Use 'mdwf_db <command> --help' for detailed help on a specific command.

Database connection: set MDWF_DB_URL environment variable
Ensemble identifiers: use ID (-e 1), path (-e ./path), or current dir (-e .)
"""

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return 1

    # Validate DB presence for commands that require an existing DB
    # Allow init-db to create a new database
    # Allow --params and --list-fields to work without database (just shows documentation)
    # Allow query/ingest without variant to show helpful error message
    skip_db_check = (
        args.cmd == 'init-db' or
        getattr(args, 'params', False) or
        getattr(args, 'list_fields', False) or
        (args.cmd in ('query', 'ingest') and not getattr(args, 'variant', None))
    )
    if not skip_db_check:
        db_conn = get_default_db_connection()
        if not db_conn:
            print("ERROR: No database connection configured.")
            print("Hint: Set MDWF_DB_URL environment variable (MongoDB).")
            return 1

    # every module must set args.func to its handler in register()
    return args.func(args)

if __name__=='__main__':
    sys.exit(main())