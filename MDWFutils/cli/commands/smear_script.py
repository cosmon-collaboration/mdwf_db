#!/usr/bin/env python3
"""
commands/smear_script.py

Generate GLU SLURM script for configuration smearing.
"""
import sys, os, ast, argparse
from pathlib import Path

from MDWFutils.db           import get_ensemble_details
from MDWFutils.jobs.smear   import generate_smear_sbatch

REQUIRED_JOB_PARAMS = ['mail_user', 'config_start', 'config_end']
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
        help='Generate GLU smearing SLURM script',
        description="""
Generate a complete SLURM script for configuration smearing using GLU.

WHAT THIS DOES:
• Creates a GLU input file with smearing parameters
• Generates a SLURM batch script for GPU execution
• Sets up proper directory structure for smeared configurations
• Configures job parameters for HPC submission

JOB PARAMETERS (via -j/--job-params):
Required parameters:
  mail_user:     Email address for job notifications
  config_start:  First configuration number to smear
  config_end:    Last configuration number to smear

Optional parameters (with defaults):
  account: m2986_g          # SLURM account
  constraint: gpu           # Node constraint
  queue: regular            # SLURM partition
  time_limit: 06:00:00      # Job time limit
  nodes: 1                  # Number of nodes
  cpus_per_task: 16         # CPUs per task
  gpus: 4                   # GPUs per node
  gpu_bind: none            # GPU binding
  ranks: 4                  # MPI ranks
  bind_sh: bind.sh          # CPU binding script

GLU PARAMETERS (via -g/--glu-params):
GLU parameters use flat names (no dots) and can be overridden:

Common smearing parameters (with defaults):
  SMEARTYPE: STOUT          # Smearing algorithm (STOUT, APE, etc.)
  SMITERS: 8                # Number of smearing iterations
  ALPHA1: 0.75              # Primary smearing parameter
  ALPHA2: 0.4               # Secondary smearing parameter
  ALPHA3: 0.2               # Tertiary smearing parameter
  
Other GLU parameters:
  CONFNO: 24                # Configuration number (overridden by range)
  DIM_0, DIM_1, DIM_2: 16   # Spatial dimensions (auto-set from ensemble)
  DIM_3: 48                 # Temporal dimension (auto-set from ensemble)
  GFTYPE: COULOMB           # Gauge fixing type
  GF_TUNE: 0.09             # Gauge fixing parameter
  ACCURACY: 14              # Gauge fixing accuracy
  MAX_ITERS: 650            # Maximum gauge fixing iterations

EXAMPLES:
  # Basic smearing job
  mdwf_db smear-script -e 1 \\
    -j "mail_user=user@example.com config_start=100 config_end=200"

  # Custom smearing parameters
  mdwf_db smear-script -e 1 \\
    -j "mail_user=user@example.com config_start=100 config_end=200 time_limit=12:00:00" \\
    -g "SMITERS=10 ALPHA1=0.8 SMEARTYPE=APE"

  # Specify output file
  mdwf_db smear-script -e 1 -o custom_smear.sh \\
    -j "mail_user=user@example.com config_start=100 config_end=200"
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e','--ensemble-id', type=int, required=True,
                   help='ID of the ensemble to generate smearing script for')
    p.add_argument('-j','--job-params', default='',
                   help=f'Space-separated key=val for SLURM job parameters. Required: {REQUIRED_JOB_PARAMS}')
    p.add_argument('-g','--glu-params', default='',
                   help='Space-separated key=val for GLU smearing parameters. Example: "SMITERS=10 ALPHA1=0.8"')
    p.add_argument('-o', '--output-file', help='Output SBATCH script path (auto-generated if not specified)')
    p.set_defaults(func=do_smear_script)

def do_smear_script(args):
    # Parse job parameters - these are ALL parameters used to generate the SLURM script
    job_dict = DEFAULT_PARAMS.copy()
    if args.job_params:
        for param in args.job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val

    # Require essential job parameters
    missing = [k for k in ('config_start','config_end','mail_user') if k not in job_dict]
    if missing:
        print(f"ERROR: missing required job parameters: {missing}", file=sys.stderr)
        return 1

    # Parse GLU parameters into flat dict
    glu_dict = {}
    if args.glu_params:
        for param in args.glu_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                try:
                    glu_dict[key] = ast.literal_eval(val)
                except:
                    glu_dict[key] = val

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
        custom_changes = glu_dict,
        **job_dict
    )
    print("Wrote smearing SBATCH script to", sbatch)
    return 0