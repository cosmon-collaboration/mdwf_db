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

REQUIRED_JOB_PARAMS = ['mail_user', 'config_start', 'config_end', 'config_inc']
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
Generate a complete SLURM script for configuration smearing using GLU.
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
            config = get_operation_config(ens_dir, 'wflow', args.params_variant)
            if config:
                config_job_params = config.get('job_params', '')
                config_glu_params = config.get('params', '')
                print(f"Loaded smearing.{args.params_variant} default parameters from {get_config_path(ens_dir)}")
            else:
                config_path = get_config_path(ens_dir)
                print(f"Warning: No wflow.{args.params_variant} default parameters found in {config_path}")
        else:
            # Try different parameter variants for smearing (fallback behavior)
            config = None
            for smear_type in ['stout8', 'stout4', 'ape', 'default']:
                config = get_operation_config(ens_dir, 'wflow', smear_type)
                if config:
                    config_job_params = config.get('job_params', '')
                    config_glu_params = config.get('params', '')
                    print(f"Loaded wflow.{smear_type} default parameters from {get_config_path(ens_dir)}")
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