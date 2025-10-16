#!/usr/bin/env python3
"""
commands/wflow_script.py

Generate SLURM script for configuration smearing.
"""
import sys, os, ast, argparse
from pathlib import Path

from MDWFutils.db           import get_ensemble_details, resolve_ensemble_identifier
from MDWFutils.jobs.wflow   import generate_wflow_sbatch
from MDWFutils.config       import get_operation_config, merge_params, get_config_path, save_operation_config

REQUIRED_JOB_PARAMS = ['config_start', 'config_end', 'config_inc']
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
        'wflow-script',
        help='Generate gradient flow SLURM script',
        description="""
Generate a complete SLURM script for gradient flow measurements using GLU.

WHAT THIS DOES:
• Creates GLU input file with gradient flow parameters
• Generates SLURM batch script for GPU execution
• Sets up proper directory structure for gradient flow output
• Configures job parameters for HPC submission

JOB PARAMETERS (via -j/--job-params):
Required parameters:
  mail_user:     Email address for job notifications
  config_start:  First configuration number to process
  config_end:    Last configuration number to process
  config_inc:    Step/increment between configurations

Optional parameters (with defaults):
  account: m2986            # SLURM account
  constraint: cpu           # Node constraint
  queue: regular            # SLURM partition
  time_limit: 01:00:00      # Job time limit
  nodes: 1                  # Number of nodes
  cpus_per_task: 256        # CPUs per task
  gpus: 4                   # GPUs per node
  gpu_bind: none            # GPU binding
  ranks: 4                  # MPI ranks
  bind_sh: bind.sh          # CPU binding script

GLU PARAMETERS (via -g/--glu-params):
GLU parameters for gradient flow measurements:

Common gradient flow parameters (with defaults):
  CONFNO: 24                # Configuration number (overridden by range)
  DIM_0, DIM_1, DIM_2: 16   # Spatial dimensions (auto-set from ensemble)
  DIM_3: 48                 # Temporal dimension (auto-set from ensemble)
  FLOWTYPE: RK4             # Flow algorithm (RK4, RK3, etc.)
  FLOWTIMES: 100            # Number of flow time steps
  FLOWSTEP: 0.01            # Flow step size
  ACCURACY: 14              # Flow accuracy
  MAX_ITERS: 1000           # Maximum flow iterations

EXAMPLES:
  # Basic gradient flow job
  mdwf_db wflow-script -e 1 \\
    -j "mail_user=user@example.com config_start=100 config_end=200 config_inc=4"

  # Custom flow parameters
  mdwf_db wflow-script -e 1 \\
    -j "mail_user=user@example.com config_start=100 config_end=200 config_inc=4 time_limit=12:00:00" \\
    -g "FLOWTIMES=200 FLOWSTEP=0.005 FLOWTYPE=RK3"

  # Specify output file
  mdwf_db wflow-script -e 1 -o custom_wflow.sh \\
    -j "mail_user=user@example.com config_start=100 config_end=200 config_inc=4"

  # Use stored default parameters
  mdwf_db wflow-script -e 1 --use-default-params

  # Use default params with CLI overrides
  mdwf_db wflow-script -e 1 --use-default-params -g "FLOWTIMES=150" -j "config_inc=4 time_limit=08:00:00"

  # Save current parameters for later reuse
  mdwf_db wflow-script -e 1 -j "mail_user=user@nersc.gov config_start=100 config_end=200 config_inc=4" --save-default-params

  # Save under custom variant name
  mdwf_db wflow-script -e 1 -g "FLOWTIMES=50" -j "config_start=100 config_end=200" --save-params-as "quick"

  # Use specific parameter variant
  mdwf_db wflow-script -e 1 --use-default-params --params-variant quick

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
                   help='Space-separated key=val for GLU gradient flow parameters. Example: "FLOWTIMES=100 FLOWSTEP=0.01"')
    p.add_argument('-o', '--output-file', help='Output SBATCH script path (auto-generated if not specified)')
    p.add_argument('--use-default-params', action='store_true',
                   help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
    p.add_argument('--params-variant',
                   help='Specify which parameter variant to use (e.g., default, quick, detailed)')
    p.add_argument('--save-default-params', action='store_true',
                   help='Save current command parameters to default parameter file for later reuse')
    p.add_argument('--save-params-as',
                   help='Save current parameters under specific variant name (default: default)')
    p.add_argument('--run-dir',
                   help='Directory to run the job from (must contain a full copy of the ensemble directory)')
    p.set_defaults(func=do_wflow_script)

def do_wflow_script(args):
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
            config = get_operation_config(ens_dir, 'wflow', args.params_variant)
            if config:
                config_job_params = config.get('job_params', '')
                config_glu_params = config.get('params', '')
                print(f"Loaded wflow.{args.params_variant} default parameters from {get_config_path(ens_dir)}")
            else:
                config_path = get_config_path(ens_dir)
                print(f"Warning: No wflow.{args.params_variant} default parameters found in {config_path}")
        else:
            # Try different parameter variants for wflow (fallback behavior)
            config = None
            for flow_type in ['default']:
                config = get_operation_config(ens_dir, 'wflow', flow_type)
                if config:
                    config_job_params = config.get('job_params', '')
                    config_glu_params = config.get('params', '')
                    print(f"Loaded wflow.{flow_type} default parameters from {get_config_path(ens_dir)}")
                    break
            
            if not config:
                config_path = get_config_path(ens_dir)
                if config_path.exists():
                    print(f"Warning: No wflow default parameters found in {config_path}")
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
    missing = [k for k in ('config_start','config_end','config_inc','mail_user') if k not in job_dict]
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
    sbatch = generate_wflow_sbatch(
        db_file       = args.db_file,
        ensemble_id   = ensemble_id,
        ensemble_dir  = str(ens_dir),
        run_dir       = args.run_dir,
        custom_changes = glu_dict,
        **job_dict
    )
    print("Wrote wflow SBATCH script to", sbatch)
    
    # Save parameters to default params if requested
    if args.save_default_params:
        save_variant = args.save_params_as if args.save_params_as else 'default'
        success = save_operation_config(
            ens_dir, 'wflow', save_variant,
            job_params=merged_job_params,
            params=merged_glu_params
        )
        if success:
            print(f"Saved parameters to {get_config_path(ens_dir)}: wflow.{save_variant}")
        else:
            print(f"Warning: Failed to save parameters to default params", file=sys.stderr)
    
    return 0