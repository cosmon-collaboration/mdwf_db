import argparse
import pkgutil
import importlib
import os
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(prog="mdwf_db")

    DEFAULT_DB = os.getenv('MDWF_DB',
                           str(Path('.').resolve()/'mdwf_ensembles.db'))
    db_parent = argparse.ArgumentParser(add_help=False)
    db_parent.add_argument(
        '--db-file',
        default=DEFAULT_DB,
        help='Path to the SQLite DB (or set MDWF_DB env).'
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

    # every module must set args.func to its handler in register()
    return args.func(args)

if __name__=='__main__':
    sys.exit(main())