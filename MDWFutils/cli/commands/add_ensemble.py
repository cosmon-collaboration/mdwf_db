#!/usr/bin/env python3
"""
commands/add_ensemble.py

Sub‐command “add-ensemble” for mdwf_db:
"""
import sys
from pathlib import Path
from MDWFutils.db import add_ensemble, update_ensemble

REQUIRED = ['beta','b','Ls','mc','ms','ml','L','T']

def register(subparsers):
    p = subparsers.add_parser(
        'add-ensemble',
        help='Add a new ensemble (all fields in --params)'
    )
    p.add_argument(
        '-p','--params',
        required=True,
        help=('Space-separated key=val pairs. '
              'Must include: beta, b, Ls, mc, ms, ml, L, T; '
              'extras allowed.')
    )
    p.add_argument(
        '-s','--status',
        required=True,
        choices=['TUNING','PRODUCTION'],
        help='Ensemble status (TUNING or PRODUCTION)'
    )
    p.add_argument(
        '-d','--directory',
        help='Explicit path (overrides auto-generated under base-dir)'
    )
    p.add_argument(
        '-b','--base-dir',
        default='.',
        help='Root containing TUNING/ and ENSEMBLES/ (default=CWD)'
    )
    p.add_argument(
        '--description',
        default=None,
        help='Optional free-form text'
    )
    p.set_defaults(func=do_add)


def do_add(args):
    #parse params into a dict
    pdict = {}
    for tok in args.params.strip().split():
        if '=' not in tok:
            print(f"ERROR: bad key=val pair '{tok}'", file=sys.stderr)
            return 1
        k, v = tok.split('=', 1)
        pdict[k] = v

    #check required keys
    missing = [k for k in REQUIRED if k not in pdict]
    if missing:
        print(f"ERROR: missing required params: {missing}", file=sys.stderr)
        return 1

    #figure out directory
    base       = Path(args.base_dir).resolve()
    tuning_root= base / 'TUNING'
    prod_root  = base / 'ENSEMBLES'
    tuning_root.mkdir(parents=True, exist_ok=True)
    prod_root.mkdir(parents=True, exist_ok=True)

    if args.directory:
        ens_dir = Path(args.directory).resolve()
    else:
        rel = (
          f"b{pdict['beta']}/b{pdict['b']}Ls{pdict['Ls']}/"
          f"mc{pdict['mc']}/ms{pdict['ms']}/ml{pdict['ml']}/"
          f"L{pdict['L']}/T{pdict['T']}"
        )
        root = prod_root if args.status=='PRODUCTION' else tuning_root
        ens_dir = root / rel

    #create folders
    ens_dir.mkdir(parents=True, exist_ok=True)
    for sub in ("log_hmc","jlog","cnfg","slurm"):
        (ens_dir / sub).mkdir(exist_ok=True)

    eid, created = add_ensemble(
        args.db_file, str(ens_dir), pdict, description=args.description
    )
    if created:
        print(f"✅ Ensemble added: ID={eid}")
    else:
        print(f"⚠ Ensemble already exists: ID={eid}")

    # if PRODUCTION, patch status & dir
    if args.status == 'PRODUCTION':
        ok = update_ensemble(
            args.db_file, eid,
            status='PRODUCTION',
            directory=str(ens_dir)
        )
        print(f"Marked PRODUCTION in DB: {'OK' if ok else 'FAIL'}")

    return 0