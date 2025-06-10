#!/usr/bin/env python3
"""
commands/smear_script.py

write GLU input + slurm sbatch for smearing.
"""
import sys, os, ast
from pathlib import Path

from MDWFutils.db           import get_ensemble_details
from MDWFutils.jobs.smear   import generate_smear_sbatch

REQUIRED_JOB_PARAMS = ['queue','time_limit','nodes','cpus_per_task']
DEFAULT_JOB_PARAMS = {
    'account'       : 'm2986',
    'constraint'    : 'cpu',
    'queue'         : 'regular',
    'time_limit'    : '0:20:00',
    'nodes'         : '1',
    'cpus_per_task' : '1',
    'job_name'      : 'glu_smear',
}

def register(subparsers):
    p = subparsers.add_parser(
        'smear-script',
        help='Build GLU‐input + SLURM sbatch in one go'
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='DB ensemble ID')
    p.add_argument('-j','--job-params', default='',
                   help=('Space‐separated key=val for SBATCH; '
                         f'required: {REQUIRED_JOB_PARAMS}'))
    p.add_argument('-s','--smear-params', default='',
                   help=('Space‐separated key=val for smearing; '
                         'must include config_start,config_end; '
                         'nest with dots: SECTION.KEY=val'))
    p.add_argument('--glu-path', required=True,
                   help='Full path to GLU executable')
    p.set_defaults(func=do_smear_script)


def do_smear_script(args):
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1
    ens_dir = str(Path(ens['directory']).resolve())

    # parse SBATCH params
    jdict = {}
    for tok in args.job_params.split():
        if '=' not in tok:
            print(f"ERROR: bad job-param {tok}", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        jdict[k] = v
    missing = [k for k in REQUIRED_JOB_PARAMS if k not in jdict]
    if missing:
        print("ERROR: missing job-params:", missing, file=sys.stderr)
        return 1
    for k,v in DEFAULT_JOB_PARAMS.items():
        jdict.setdefault(k,v)
    # cast numerics
    jdict['nodes']         = int(jdict['nodes'])
    jdict['cpus_per_task'] = int(jdict['cpus_per_task'])

    # parse smear‐params into nested dict
    sdict = {}
    for tok in args.smear_params.split():
        if '=' not in tok:
            print(f"ERROR: bad smear-param {tok}", file=sys.stderr)
            return 1
        key, raw = tok.split('=',1)
        try:
            val = ast.literal_eval(raw)
        except:
            val = raw
        parts = key.split('.')
        d = sdict
        for p in parts[:-1]:
            d = d.setdefault(p,{})
        d[parts[-1]] = val

    # require config_start & config_end
    if 'config_start' not in sdict or 'config_end' not in sdict:
        print("ERROR: must supply config_start and config_end", file=sys.stderr)
        return 1
    cs = int(sdict.pop('config_start'))
    ce = int(sdict.pop('config_end'))

    sbatch = generate_smear_sbatch(
        db_file       = args.db_file,
        ensemble_id   = args.ensemble_id,
        ensemble_dir  = ens_dir,
        glu_path      = args.glu_path,

        # SBATCH args
        **jdict,

        # smearing args
        config_start   = cs,
        config_end     = ce,
        custom_changes = sdict
    )
    print("Wrote SBATCH →", sbatch)
    return 0