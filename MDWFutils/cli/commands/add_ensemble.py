#!/usr/bin/env python3
"""
commands/add_ensemble.py

Sub‐command "add-ensemble" for mdwf_db:
"""
import sys
import re
from pathlib import Path
from MDWFutils.db import add_ensemble, update_ensemble, set_ensemble_parameter

REQUIRED = ['beta','b','Ls','mc','ms','ml','L','T']

def register(subparsers):
    p = subparsers.add_parser(
        'add-ensemble',
        help='Add a new ensemble to the database',
        description="""
Add a new ensemble to the MDWF database. This command:
1. Creates the ensemble directory structure
2. Adds the ensemble record to the database
3. Sets up initial operation tracking

Required parameters:
- beta: Gauge coupling
- b: Domain wall height
- Ls: Domain wall extent
- mc: Charm quark mass
- ms: Strange quark mass
- ml: Light quark mass
- L: Spatial lattice size
- T: Temporal lattice size

The ensemble directory will be created under:
- TUNING/ for status=TUNING
- ENSEMBLES/ for status=PRODUCTION

Directory structure:
  <base_dir>/<status>/b<beta>/b<b>Ls<Ls>/mc<mc>/ms<ms>/ml<ml>/L<L>/T<T>/

You can either:
  • provide physics parameters with -p/--params, or
  • provide an explicit --directory that follows the structure above and the
    parameters will be inferred from the path when -p is omitted.
"""
    )
    p.add_argument(
        '-p','--params',
        required=False,
        help=('Space-separated key=val pairs. Required if --directory is not given or does not encode all values: '
              'beta, b, Ls, mc, ms, ml, L, T. '
              'Example: "beta=6.0 b=1.8 Ls=24 mc=0.8555 ms=0.0725 ml=0.0195 L=32 T=64"')
    )
    p.add_argument(
        '-s','--status',
        required=True,
        choices=['TUNING','PRODUCTION'],
        help='Ensemble status: TUNING for development, PRODUCTION for final runs'
    )
    p.add_argument(
        '-d','--directory',
        help='Explicit path to ensemble directory (overrides auto-generated path)'
    )
    p.add_argument(
        '-b','--base-dir',
        default='.',
        help='Root directory containing TUNING/ and ENSEMBLES/ (default: current directory)'
    )
    p.add_argument(
        '--description',
        default=None,
        help='Optional free-form text description of the ensemble'
    )
    p.add_argument(
        '--nickname',
        default=None,
        help='Optional nickname to attach to this ensemble for quick lookup'
    )
    p.set_defaults(func=do_add)


def _parse_params_from_path(path: Path):
    """Infer physics parameters from an ensemble directory path.
    Expected segments like: b<beta>/b<b>Ls<Ls>/mc<mc>/ms<ms>/ml<ml>/L<L>/T<T>
    Returns dict with any found keys.
    """
    s = str(path)
    patterns = {
        'beta': r'b(\d+\.?\d*)',
        'b':    r'b(\d+\.?\d*)Ls',
        'Ls':   r'Ls(\d+)',
        'mc':   r'mc(\d+\.?\d*)',
        'ms':   r'ms(\d+\.?\d*)',
        'ml':   r'ml(\d+\.?\d*)',
        'L':    r'L(\d+)',
        'T':    r'T(\d+)',
    }
    out = {}
    for key, pat in patterns.items():
        m = re.search(pat, s)
        if m:
            out[key] = m.group(1)
    return out


def do_add(args):
    # parse params into a dict (if provided)
    pdict = {}
    if args.params:
        for tok in args.params.strip().split():
            if '=' not in tok:
                print(f"ERROR: bad key=val pair '{tok}'", file=sys.stderr)
                return 1
            k, v = tok.split('=', 1)
            pdict[k] = v

    # determine target ensemble directory (may need params to construct)
    base       = Path(args.base_dir).resolve()

    if args.directory:
        ens_dir = Path(args.directory).resolve()
        # If params were not supplied, try to infer them from the provided directory
        if not pdict:
            inferred = _parse_params_from_path(ens_dir)
            pdict.update(inferred)
    else:
        # Only create TUNING/ and ENSEMBLES/ roots when auto-generating the directory
        tuning_root= base / 'TUNING'
        prod_root  = base / 'ENSEMBLES'
        tuning_root.mkdir(parents=True, exist_ok=True)
        prod_root.mkdir(parents=True, exist_ok=True)
        rel = (
          f"b{pdict['beta']}/b{pdict['b']}Ls{pdict['Ls']}/"
          f"mc{pdict['mc']}/ms{pdict['ms']}/ml{pdict['ml']}/"
          f"L{pdict['L']}/T{pdict['T']}"
        )
        root = prod_root if args.status=='PRODUCTION' else tuning_root
        ens_dir = root / rel

    # final required-keys check (either provided or inferred)
    missing = [k for k in REQUIRED if k not in pdict]
    if missing:
        print(f"ERROR: missing required params: {missing}. Provide -p or use --directory with a standard path.", file=sys.stderr)
        return 1

    #create folders
    ens_dir.mkdir(parents=True, exist_ok=True)
    for sub in ("log_hmc","jlog","cnfg","slurm"):
        (ens_dir / sub).mkdir(exist_ok=True)

    eid, created = add_ensemble(
        args.db_file, str(ens_dir), pdict, description=args.description
    )
    if created:
        print(f"Ensemble added: ID={eid}")
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

    # Set optional nickname
    if getattr(args, 'nickname', None):
        try:
            set_ensemble_parameter(args.db_file, eid, 'nickname', args.nickname)
            print(f"Set nickname: {args.nickname}")
        except Exception as e:
            print(f"WARNING: Failed to set nickname: {e}", file=sys.stderr)

    return 0