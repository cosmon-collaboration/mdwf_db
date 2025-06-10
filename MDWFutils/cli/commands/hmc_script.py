#!/usr/bin/env python3
"""
commands/hmc_script.py

Generate HMC XML + GPU SBATCH script for an ensemble, with
required & defaulted job-params.
"""
import sys, os
from pathlib import Path

from MDWFutils.db            import get_ensemble_details
from MDWFutils.jobs.hmc      import generate_hmc_parameters, generate_hmc_slurm_gpu

REQUIRED_JOB_PARAMS = ['queue','cfg_max']
DEFAULT_JOB_PARAMS = {
    'constraint'    : 'gpu',
    'time_limit'    : '17:00:00',
    'cpus_per_task' : '32',
    'nodes'         : '1',
    'gpus_per_task' : '1',
    'gpu_bind'      : 'none',
    'mail_user'     : os.getenv('USER',''),
    'mpi'           : '2.1.1.2',
}

def register(subparsers):
    p = subparsers.add_parser('hmc-script',
        help='Generate HMC XML + GPU SBATCH script for an ensemble'
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble')
    p.add_argument('-a','--account', required=True,
                   help='SLURM account name')
    p.add_argument('-m','--mode',
                   choices=['tepid','continue','reseed'],
                   required=True,
                   help='Which HMC XML & run mode to use')
    p.add_argument('--base-dir', default='.',
                   help='Root containing TUNING/ & ENSEMBLES/ (default=CWD)')
    p.add_argument('-x','--xml-params', default='',
                   help='Space‐separated key=val for XML overrides')
    p.add_argument('-j','--job-params', default='',
                   help='Space‐separated key=val for SBATCH/job script')
    p.set_defaults(func=do_hmc_script)


def do_hmc_script(args):
    # Load ensemble record
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1
    ens_dir = Path(ens['directory']).resolve()

    base = Path(args.base_dir).resolve()
    try:
        rel = ens_dir.relative_to(base)
    except ValueError:
        print(f"ERROR: {ens_dir} is not under base-dir {base}", file=sys.stderr)
        return 1
    root     = rel.parts[0]           # "TUNING" or "ENSEMBLES"
    ens_rel  = str(rel)               # e.g. "TUNING/b6.0/.../T32"
    ens_name = ens_rel.replace('/','_')

    #  Ensure slurm folder & output path
    slurm_dir = ens_dir / 'slurm'
    slurm_dir.mkdir(parents=True, exist_ok=True)
    out_file = slurm_dir / f"hmc_{args.ensemble_id}_{args.mode}.sbatch"

    # XML generation
    xdict = {}
    for tok in args.xml_params.split():
        if '=' not in tok:
            print(f"ERROR: bad XML param '{tok}'", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        xdict[k] = v
    generate_hmc_parameters(str(ens_dir), mode=args.mode, **xdict)

    # Parse & validate job-params
    jdict = {}
    for tok in args.job_params.split():
        if '=' not in tok:
            print(f"ERROR: bad job param '{tok}'", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        jdict[k] = v

    missing = [k for k in REQUIRED_JOB_PARAMS if k not in jdict]
    if missing:
        print("ERROR: missing job parameters:", missing, file=sys.stderr)
        return 1

    # fill defaults for all the rest
    for k, v in DEFAULT_JOB_PARAMS.items():
        jdict.setdefault(k, v)

    # default ntasks_per_node => cpus_per_task
    if 'ntasks_per_node' not in jdict:
        jdict['ntasks_per_node'] = jdict['cpus_per_task']

    # if user never asked for resubmit, disable it in reseed mode
    if 'resubmit' not in jdict:
        jdict['resubmit'] = 'false' if args.mode=='reseed' else 'true'

    # Assemble arguments & call the SBATCH-generator
    sbatch_args = {
        'out_path'    : str(out_file),
        'db_file'     : args.db_file,
        'ensemble_id' : args.ensemble_id,
        'base_dir'    : args.base_dir,
        'type_'       : root,
        'ens_relpath' : ens_rel,
        'ens_name'    : ens_name,
        'account'     : args.account,
        'mode'        : args.mode,
    }
    # everything from jdict (constraint, queue, time_limit, mpi, etc.)
    sbatch_args.update(jdict)

    generate_hmc_slurm_gpu(**sbatch_args)

    print("Wrote HMC sbatch ->", out_file)
    return 0