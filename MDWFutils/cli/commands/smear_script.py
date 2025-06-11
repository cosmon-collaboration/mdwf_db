#!/usr/bin/env python3
"""
commands/smear_script.py

write GLU input + slurm sbatch for smearing.
"""
import sys, os, ast
from pathlib import Path
import click

from MDWFutils.db           import get_ensemble_details
from MDWFutils.jobs.smear   import generate_smear_sbatch

REQUIRED_JOB_PARAMS = ['mail_user']
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
    p = subparsers.add_parser(
        'smear-script',
        help='Generate GLU SLURM script for smearing'
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble')
    p.add_argument('-j','--job-params', default='',
                   help=('Space-separated key=val for SBATCH; '
                         f'required: {REQUIRED_JOB_PARAMS}'))
    p.add_argument('-g','--glu-params', default='',
                   help=('Space-separated key=val for GLU input; '
                         'must include config_start,config_end; '
                         'nest with dots: SECTION.KEY=val'))
    p.add_argument('-o', '--output-file', help='Output SBATCH script path')
    p.set_defaults(func=do_smear_script)

def do_smear_script(args):
    # Parse job parameters
    job_dict = DEFAULT_PARAMS.copy()
    if args.job_params:
        for param in args.job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val

    # List of valid job parameters for generate_smear_sbatch
    valid_job_params = {'db_file','ensemble_id','ensemble_dir','glu_path','account','constraint','queue','time_limit','job_name','nodes','cpus_per_task','gpus','gpu_bind','ranks','bind_sh','mail_user','config_start','config_end','config_prefix','output_prefix','SMEARTYPE','SMITERS','alpha_values','config_inc','nsim','custom_changes','output_file'}
    unused = [k for k in job_dict if k not in valid_job_params]
    for k in unused:
        print(f"WARNING: job parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused:
        job_dict.pop(k)

    # Require config_start & config_end in job_dict
    if 'config_start' not in job_dict or 'config_end' not in job_dict:
        print("ERROR: must supply config_start and config_end in job parameters", file=sys.stderr)
        return 1

    # Parse GLU parameters into nested dict
    gdict = {}
    for tok in args.glu_params.split():
        if '=' not in tok:
            print(f"ERROR: bad GLU-param {tok}", file=sys.stderr)
            return 1
        key, raw = tok.split('=',1)
        try:
            val = ast.literal_eval(raw)
        except:
            val = raw
        parts = key.split('.')
        d = gdict
        for p in parts[:-1]:
            d = d.setdefault(p,{})
        d[parts[-1]] = val

    # List of valid GLU parameters (keys in gdict)
    valid_glu_params = {'config_number','lattice_dims','MODE','SMEARTYPE','SMITERS','alpha_values'}
    unused_g = [k for k in gdict if k not in valid_glu_params]
    for k in unused_g:
        print(f"WARNING: GLU parameter '{k}' was provided but is not used by the script generator.", file=sys.stderr)
    # Remove unused keys
    for k in unused_g:
        gdict.pop(k)

    # Get ensemble directory
    ens = get_ensemble_details(args.db_file, args.ensemble_id)
    if not ens:
        print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
        return 1
    ens_dir = str(Path(ens['directory']).resolve())

    # Generate the script
    sbatch = generate_smear_sbatch(
        db_file       = args.db_file,
        ensemble_id   = args.ensemble_id,
        ensemble_dir  = ens_dir,
        custom_changes = gdict,
        **job_dict
    )
    print("Wrote smearing SBATCH script →", sbatch)
    return 0

@click.command()
@click.option('--ensemble-id', '-e', required=True, help='Ensemble ID')
@click.option('--output-file', '-o', help='Output SBATCH script path')
@click.option('--job-params', '-j', required=True, help='Job parameters in format "key1=val1 key2=val2" (mail_user is required)')
@click.option('--smear-params', '-s', help='Smearing parameters in format "key1=val1 key2=val2"')
def smear_script(ensemble_id, output_file, job_params, smear_params):
    """Generate a smearing SBATCH script."""
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

    # Parse smearing parameters
    smear_dict = {}
    if smear_params:
        for param in smear_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                smear_dict[key] = val

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
    generate_smear_sbatch(
        output_file=output_file,
        db_file=get_db_path(),
        ensemble_id=ensemble_id,
        ensemble_dir=ensemble_dir,
        custom_changes=smear_dict,
        **job_dict
    )