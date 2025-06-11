#!/usr/bin/env python3
"""
commands/hmc_script.py

Generate HMC XML + GPU SBATCH script for an ensemble, with
required & defaulted job-params.
"""
import sys, os
from pathlib import Path
import click

from MDWFutils.db            import get_ensemble_details
from MDWFutils.jobs.hmc      import generate_hmc_parameters, generate_hmc_slurm_gpu

REQUIRED_JOB_PARAMS = ['cfg_max']
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

DEFAULT_PARAMS = {
    'account': 'm2986_g',
    'constraint': 'gpu',
    'queue': 'regular',
    'time_limit': '06:00:00',
    'nodes': 1,
    'cpus_per_task': 16,
    'gpus': 4,
    'gpu_bind': 'none',
    'ranks': 4,
    'bind_sh': 'bind.sh'
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
                   help='Space‐separated key=val for SBATCH/job script. Required params: queue, cfg_max, exec_path, bind_script')
    p.add_argument('-o','--output-file', help='Output SBATCH script path')
    p.add_argument('--hmc-params', help='HMC parameters in format "key1=val1 key2=val2"')
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
    ens_name = ens_rel.replace('TUNING/', '').replace('ENSEMBLES/', '').replace('/', '_')

    #  Ensure slurm folder & output path
    slurm_dir = ens_dir / 'slurm'
    slurm_dir.mkdir(parents=True, exist_ok=True)
    out_file = args.output_file or slurm_dir / f"hmc_{args.ensemble_id}_{args.mode}.sbatch"

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

    # Warn for unused job parameters
    valid_job_params = {'constraint', 'time_limit', 'cpus_per_task', 'nodes', 'gpus_per_task', 'gpu_bind', 'mail_user', 'mpi', 'account', 'queue', 'ntasks_per_node', 'resubmit', 'bind_sh', 'ranks', 'output_file', 'cfg_max', 'exec_path', 'bind_script', 'n_trajec'}
    unused = [k for k in jdict if k not in valid_job_params]
    for k in unused:
        print(f"WARNING: job parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused:
        jdict.pop(k)

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

    # Parse HMC parameters
    hmc_dict = {}
    hmc_params = args.hmc_params or ''
    for tok in hmc_params.split():
        if '=' not in tok:
            print(f"ERROR: bad HMC param '{tok}'", file=sys.stderr)
            return 1
        k,v = tok.split('=',1)
        hmc_dict[k] = v

    # Warn for unused HMC parameters
    valid_hmc_params = {'StartTrajectory','Trajectories','RandomSeed','Integrator','IntegratorSteps','IntegratorStepSize','HasenbuschMass','HasenbuschMu','HasenbuschRho','HasenbuschAlpha','HasenbuschBeta','HasenbuschGamma','HasenbuschDelta','HasenbuschEpsilon','HasenbuschZeta','HasenbuschEta','HasenbuschTheta','HasenbuschLambda','HasenbuschKappa','HasenbuschNu','HasenbuschXi','HasenbuschOmicron','HasenbuschPi','HasenbuschRho','HasenbuschSigma','HasenbuschTau','HasenbuschUpsilon','HasenbuschPhi','HasenbuschChi','HasenbuschPsi','HasenbuschOmega'}
    unused_hmc = [k for k in hmc_dict if k not in valid_hmc_params]
    for k in unused_hmc:
        print(f"WARNING: HMC parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused_hmc:
        hmc_dict.pop(k)

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
    sbatch_args.update(hmc_dict)

    generate_hmc_slurm_gpu(**sbatch_args)

    print("Wrote HMC sbatch ->", out_file)
    return 0

@click.command()
@click.option('--ensemble-id', '-e', required=True, help='Ensemble ID')
@click.option('--output-file', '-o', help='Output SBATCH script path')
@click.option('--job-params', '-j', required=True, help='Job parameters in format "key1=val1 key2=val2" (mail_user is required)')
@click.option('--hmc-params', '-h', help='HMC parameters in format "key1=val1 key2=val2"')
def hmc_script(ensemble_id, output_file, job_params, hmc_params):
    """Generate an HMC SBATCH script."""
    # Parse job parameters
    job_dict = DEFAULT_PARAMS.copy()
    if job_params:
        for param in job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val

    # Check for required mail_user
    if 'mail_user' not in job_dict:
        raise click.ClickException("mail_user is required in job parameters")

    # Parse HMC parameters
    hmc_dict = {}
    if hmc_params:
        for param in hmc_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                hmc_dict[key] = val

    # Get ensemble directory
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT directory FROM ensembles WHERE id = ?", (ensemble_id,))
    result = c.fetchone()
    conn.close()

    if not result:
        raise click.ClickException(f"Ensemble {ensemble_id} not found")
    ensemble_dir = result[0]

    # Generate SBATCH script
    generate_hmc_sbatch(
        output_file=output_file,
        db_file=get_db_path(),
        ensemble_id=ensemble_id,
        ensemble_dir=ensemble_dir,
        custom_changes=hmc_dict,
        **job_dict
    )