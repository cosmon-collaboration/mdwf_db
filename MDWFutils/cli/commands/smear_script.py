#!/usr/bin/env python3
"""
commands/smear_script.py

Generate GLU SLURM script for configuration smearing.
"""
import sys, os, ast, argparse
from pathlib import Path

from MDWFutils.db           import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.jobs.smear   import generate_smear_sbatch
from MDWFutils.config       import get_operation_config, merge_params, get_config_path, save_operation_config

REQUIRED_JOB_PARAMS = ['mail_user', 'config_start', 'config_end']
DEFAULT_PARAMS = {
    'account': 'm2986',
    'constraint': 'cpu',
    'queue': 'regular',
    'time_limit': '01:00:00',
    'nodes': 1,
    'cpus_per_task': 256,
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

  # Use stored default parameters
  mdwf_db smear-script -e 1 --use-default-params

  # Use default params with CLI overrides
  mdwf_db smear-script -e 1 --use-default-params -g "SMITERS=12" -j "time_limit=08:00:00"

  # Save current parameters for later reuse
  mdwf_db smear-script -e 1 -j "mail_user=user@nersc.gov config_start=100 config_end=200" --save-default-params

  # Save under custom variant name
  mdwf_db smear-script -e 1 -g "SMITERS=4" -j "config_start=100 config_end=200" --save-params-as "stout4"

  # Use specific parameter variant
  mdwf_db smear-script -e 1 --use-default-params --params-variant stout4

DEFAULT PARAMETER FILES:
Use 'mdwf_db default_params generate -e <ensemble>' to create a default parameter template.
The --use-default-params flag loads parameters from mdwf_default_params.yaml in the ensemble directory.
The --save-default-params flag saves current parameters to the default params file for later reuse.
CLI parameters override default parameter file parameters.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Flexible identifier (preferred)
    p.add_argument('-e','--ensemble', required=False,
                   help='Ensemble ID, directory path, or "." for current directory')
    # Legacy integer-only option for backward compatibility
    p.add_argument('--ensemble-id', dest='ensemble_id', type=int, required=False,
                   help='[DEPRECATED] Ensemble ID (use -e/--ensemble for flexible ID or path)')
    p.add_argument('-j','--job-params', default='',
                   help=f'Space-separated key=val for SLURM job parameters. Required: {REQUIRED_JOB_PARAMS}')
    p.add_argument('-g','--glu-params', default='',
                   help='Space-separated key=val for GLU smearing parameters. Example: "SMITERS=10 ALPHA1=0.8"')
    p.add_argument('-o', '--output-file', help='Output SBATCH script path (auto-generated if not specified)')
    p.add_argument('--use-default-params', action='store_true',
                   help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
    p.add_argument('--params-variant',
                   help='Specify which parameter variant to use (e.g., stout8, stout4, ape)')
    p.add_argument('--save-default-params', action='store_true',
                   help='Save current command parameters to default parameter file for later reuse')
    p.add_argument('--save-params-as',
                   help='Save current parameters under specific variant name (default: stout8)')
    p.set_defaults(func=do_smear_script)

def do_smear_script(args):
    # Resolve ensemble from flexible identifier first, then legacy --ensemble-id
    ensemble_id = None
    ens = None
    if getattr(args, 'ensemble', None):
        eid, info = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if eid is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}", file=sys.stderr)
            return 1
        ensemble_id, ens = eid, info
    elif getattr(args, 'ensemble_id', None) is not None:
        ens = get_ensemble_details(args.db_file, args.ensemble_id)
        if not ens:
            print(f"ERROR: ensemble {args.ensemble_id} not found", file=sys.stderr)
            return 1
        ensemble_id = args.ensemble_id
    else:
        print("ERROR: Missing ensemble identifier. Use -e/--ensemble (ID or path) or --ensemble-id.", file=sys.stderr)
        return 1
    ens_dir = Path(ens['directory']).resolve()

    # Load parameters from config file if requested
    config_job_params = ""
    config_glu_params = ""
    
    if args.use_default_params:
        if args.params_variant:
            # Use specified variant
            config = get_operation_config(ens_dir, 'smearing', args.params_variant)
            if config:
                config_job_params = config.get('job_params', '')
                config_glu_params = config.get('params', '')
                print(f"Loaded smearing.{args.params_variant} default parameters from {get_config_path(ens_dir)}")
            else:
                config_path = get_config_path(ens_dir)
                print(f"Warning: No smearing.{args.params_variant} default parameters found in {config_path}")
        else:
            # Try different parameter variants for smearing (fallback behavior)
            config = None
            for smear_type in ['stout8', 'stout4', 'ape', 'default']:
                config = get_operation_config(ens_dir, 'smearing', smear_type)
                if config:
                    config_job_params = config.get('job_params', '')
                    config_glu_params = config.get('params', '')
                    print(f"Loaded smearing.{smear_type} default parameters from {get_config_path(ens_dir)}")
                    break
            
            if not config:
                config_path = get_config_path(ens_dir)
                if config_path.exists():
                    print(f"Warning: No smearing default parameters found in {config_path}")
                else:
                    print(f"Warning: No default parameter file found at {config_path}")
                    print("Use 'mdwf_db default_params generate' to create one")

    # Merge config parameters with CLI parameters (CLI takes precedence)
    merged_job_params = merge_params(config_job_params, args.job_params)
    merged_glu_params = merge_params(config_glu_params, args.glu_params)

    # Parse merged job parameters
    job_dict = DEFAULT_PARAMS.copy()
    if merged_job_params:
        for param in merged_job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val

    # Require essential job parameters
    missing = [k for k in ('config_start','config_end','mail_user') if k not in job_dict]
    if missing:
        if args.use_default_params:
            print(f"ERROR: missing required job parameters: {missing}. Add them to your default parameter file or use -j", file=sys.stderr)
        else:
            print(f"ERROR: missing required job parameters: {missing}", file=sys.stderr)
        return 1

    # Parse merged GLU parameters into flat dict
    glu_dict = {}
    if merged_glu_params:
        for param in merged_glu_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                try:
                    glu_dict[key] = ast.literal_eval(val)
                except:
                    glu_dict[key] = val

    # Generate the script
    sbatch = generate_smear_sbatch(
        db_file       = args.db_file,
        ensemble_id   = ensemble_id,
        ensemble_dir  = str(ens_dir),
        custom_changes = glu_dict,
        **job_dict
    )
    print("Wrote smearing SBATCH script to", sbatch)
    
    # Save parameters to default params if requested
    if args.save_default_params:
        save_variant = args.save_params_as if args.save_params_as else 'stout8'
        success = save_operation_config(
            ens_dir, 'smearing', save_variant,
            job_params=merged_job_params,
            params=merged_glu_params
        )
        if success:
            print(f"Saved parameters to default params: smearing.{save_variant}")
        else:
            print(f"Warning: Failed to save parameters to default params", file=sys.stderr)
    
    return 0