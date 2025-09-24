#!/usr/bin/env python3
"""
commands/hmc_script.py

Generate HMC XML and SLURM script for gauge configuration generation.
"""
import argparse
import os
import sys
from pathlib import Path
from MDWFutils.db import get_ensemble_details, resolve_ensemble_identifier, get_connection
from MDWFutils.jobs.hmc import generate_hmc_parameters, generate_hmc_slurm_gpu
from MDWFutils.config import get_operation_config, merge_params, get_config_path, save_operation_config

def register(subparsers):
    p = subparsers.add_parser(
        'hmc-script',
        help='Generate HMC XML and SLURM script for gauge generation',
        description="""
Generate HMC XML parameters and SLURM batch script for gauge configuration generation.

WHAT THIS DOES:
• Creates HMC XML parameter file with physics parameters
• Generates SLURM batch script for GPU execution
• Sets up proper directory structure and file paths
• Configures job parameters for HPC submission

HMC MODES:
  tepid:    Initial thermalization run (TepidStart)
  continue: Continue from existing checkpoint (CheckpointStart)
  reseed:   Start new run with different seed (CheckpointStartReseed)

JOB PARAMETERS (via -j/--job-params):
Required parameters:
  cfg_max:     Maximum configuration number to generate

Optional parameters (with defaults):
  constraint: gpu           # Node constraint
  time_limit: 17:00:00      # Job time limit
  cpus_per_task: 32         # CPUs per task
  nodes: 1                  # Number of nodes
  gpus_per_task: 1          # GPUs per task
  gpu_bind: none            # GPU binding
  mail_user: (from env)     # Email notifications
  queue: regular            # SLURM partition
  exec_path: (auto)         # Path to HMC executable
  bind_script: (auto)       # CPU binding script

XML PARAMETERS (via -x/--xml-params):
Available HMC parameters:
  StartTrajectory:      Starting trajectory number (default: 0)
  Trajectories:         Number of trajectories to generate (default: 50)
  MetropolisTest:       Perform Metropolis test (true/false, default: true)
  NoMetropolisUntil:    Trajectory to start Metropolis (default: 0)
  PerformRandomShift:   Perform random shift (true/false, default: true)
  StartingType:         Start type (TepidStart/CheckpointStart/CheckpointStartReseed)
  Seed:                 Random seed (for reseed mode)
  MDsteps:              Number of MD steps (default: 2)
  trajL:                Trajectory length (default: 1.0)

EXAMPLES:
  # Basic HMC script for new ensemble
  mdwf_db hmc-script -e 1 -a m2986 -m tepid -j "cfg_max=100"

  # Continue existing run
  mdwf_db hmc-script -e 1 -a m2986 -m continue \\
    -j "cfg_max=200 time_limit=24:00:00" \\
    -x "StartTrajectory=100 Trajectories=100"

  # Custom parameters and output file
  mdwf_db hmc-script -e 1 -a m2986 -m tepid -o custom_hmc.sh \\
    -j "cfg_max=50 nodes=2 time_limit=12:00:00" \\
    -x "MDsteps=4 trajL=0.75 Seed=12345"

  # Use stored default parameters
  mdwf_db hmc-script -e 1 -a m2986 -m tepid --use-default-params

  # Use default params with CLI overrides
  mdwf_db hmc-script -e 1 -a m2986 -m continue --use-default-params -j "nodes=2"

  # Save current parameters for later reuse
  mdwf_db hmc-script -e 1 -a m2986 -m tepid -j "cfg_max=100" -x "MDsteps=4" --save-default-params

  # Save parameters under custom variant name
  mdwf_db hmc-script -e 1 -a m2986 -m tepid -j "cfg_max=50" --save-params-as "short_run"

  # Use specific parameter variant (not just current mode)
  mdwf_db hmc-script -e 1 -a m2986 -m continue --use-default-params --params-variant tepid

DEFAULT PARAMETER FILES:
Use 'mdwf_db default_params generate -e <ensemble>' to create a default parameter template.
The --use-default-params flag loads parameters from mdwf_default_params.yaml in the ensemble directory.
The --save-default-params flag saves current parameters to the default params file for later reuse.
CLI parameters override default parameter file parameters.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    # Flexible identifier (preferred)
    p.add_argument('-e', '--ensemble', required=False,
                   help='Ensemble ID, directory path, or "." for current directory')
    # Legacy integer-only option for backward compatibility
    p.add_argument('--ensemble-id', dest='ensemble_id', type=int, required=False,
                   help='[DEPRECATED] Ensemble ID (use -e/--ensemble for flexible ID or path)')
    p.add_argument('-a', '--account', required=True,
                   help='SLURM account name (e.g., m2986)')
    p.add_argument('-m', '--mode', required=True,
                   choices=['tepid', 'continue', 'reseed'],
                   help='HMC run mode: tepid (new), continue (existing), or reseed (new seed)')
    p.add_argument('--base-dir', default='.',
                   help='Root directory containing TUNING/ and ENSEMBLES/ (default: current directory)')
    p.add_argument('-x', '--xml-params', required=True,
                   help='Space-separated key=val pairs for HMC XML parameters. Required: trajL, lvl_sizes')
    p.add_argument('-j', '--job-params', default='',
                   help='Space-separated key=val pairs for SLURM job parameters. Required: cfg_max')
    p.add_argument('-o', '--output-file',
                   help='Output SBATCH script path (auto-generated if not specified)')
    p.add_argument('--exec-path',
                   help='Path to HMC executable (saved to DB as hmc_exec_path)')
    p.add_argument('--bind-script',
                   help='Path to core binding script (saved to DB as hmc_bind_script)')
    p.add_argument('--run-dir',
                   help='Directory to run the job from (must contain a full copy of the ensemble directory)')
    p.add_argument('--use-default-params', action='store_true',
                   help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
    p.add_argument('--params-variant',
                   help='Specify which parameter variant to use (overrides default lookup). For HMC: tepid, continue, reseed')
    p.add_argument('--save-default-params', action='store_true',
                   help='Save current command parameters to default parameter file for later reuse')
    p.add_argument('--save-params-as',
                   help='Save current parameters under specific variant name (default: use current mode)')
    p.set_defaults(func=do_hmc_script)

def do_hmc_script(args):
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
        # legacy path
        ens = get_ensemble_details(args.db_file, args.ensemble_id)
        if not ens:
            print(f"ERROR: Ensemble {args.ensemble_id} not found", file=sys.stderr)
            return 1
        ensemble_id = args.ensemble_id
    else:
        print("ERROR: Missing ensemble identifier. Use -e/--ensemble (ID or path) or --ensemble-id.", file=sys.stderr)
        return 1
    
    # Get ensemble directory for config loading
    ens_dir = Path(ens['directory']).resolve()
    
    # Load parameters from default params file if requested
    config_xml_params = ""
    config_job_params = ""
    
    if args.use_default_params:
        # Use specified variant or default to current mode
        config_variant = args.params_variant if args.params_variant else args.mode
        config = get_operation_config(ens_dir, 'hmc', config_variant)
        if config:
            config_xml_params = config.get('xml_params', '')
            config_job_params = config.get('job_params', '')
            variant_msg = f" (variant: {config_variant})" if args.params_variant else ""
            print(f"Loaded HMC {config_variant} default parameters from {get_config_path(ens_dir)}{variant_msg}")
        else:
            config_path = get_config_path(ens_dir)
            if config_path.exists():
                print(f"Warning: No HMC {config_variant} default parameters found in {config_path}")
            else:
                print(f"Warning: No default parameter file found at {config_path}")
                print("Use 'mdwf_db default_params generate' to create one")
    
    # Merge config parameters with CLI parameters (CLI takes precedence)
    merged_xml_params = merge_params(config_xml_params, args.xml_params)
    merged_job_params = merge_params(config_job_params, args.job_params)
    
    # Parse merged job parameters
    job_dict = {}
    if merged_job_params:
        for param in merged_job_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                job_dict[key] = val
    
    # Check required job parameters
    if 'cfg_max' not in job_dict:
        if args.use_default_params:
            print("ERROR: cfg_max is required. Add it to your default parameter file or use -j 'cfg_max=N'", file=sys.stderr)
        else:
            print("ERROR: cfg_max is required in job parameters", file=sys.stderr)
        return 1
    
    # Parse merged XML parameters
    xml_dict = {}
    if merged_xml_params:
        for param in merged_xml_params.split():
            if '=' in param:
                key, val = param.split('=', 1)
                xml_dict[key] = val
    
    # Check required XML parameters
    missing_params = []
    if 'trajL' not in xml_dict:
        missing_params.append('trajL')
    if 'lvl_sizes' not in xml_dict:
        missing_params.append('lvl_sizes')
    
    if missing_params:
        if args.use_default_params:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}. Add them to your default parameter file or use -x '{' '.join(missing_params)}'", file=sys.stderr)
        else:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}", file=sys.stderr)
        return 1
    
    # Get base directory and compute relative paths
    base = Path(args.base_dir).resolve()
    
    try:
        rel = ens_dir.relative_to(base)
    except ValueError:
        print(f"ERROR: {ens_dir} is not under base-dir {base}", file=sys.stderr)
        return 1
    
    root = rel.parts[0]  # "TUNING" or "ENSEMBLES"
    ens_rel = str(rel)   # e.g. "TUNING/b6.0/.../T32"
    ens_name = ens_rel.replace('TUNING/', '').replace('ENSEMBLES/', '').replace('/', '_')
    
    # Ensure slurm folder & output path
    slurm_dir = ens_dir / 'slurm'
    slurm_dir.mkdir(parents=True, exist_ok=True)
    out_file = args.output_file or slurm_dir / f"hmc_{ensemble_id}_{args.mode}.sbatch"
    
    # Generate HMC XML parameters
    try:
        generate_hmc_parameters(str(ens_dir), mode=args.mode, **xml_dict)
    except Exception as e:
        print(f"ERROR: Failed to generate HMC XML: {e}", file=sys.stderr)
        return 1
    
    # Set up job parameters with defaults
    job_params = {
        'constraint': 'gpu',
        'time_limit': '17:00:00',
        'cpus_per_task': '32',
        'nodes': '1',
        'gpus_per_task': '1',
        'gpu_bind': 'none',
        'mail_user': os.getenv('USER', ''),
        'mpi': '2.1.1.2',
    }
    job_params.update(job_dict)
    
    # Set ntasks_per_node if not provided
    if 'ntasks_per_node' not in job_params:
        job_params['ntasks_per_node'] = job_params['cpus_per_task']
    
    # Set resubmit flag
    if 'resubmit' not in job_params:
        job_params['resubmit'] = 'false' if args.mode == 'reseed' else 'true'
    
    # Generate SLURM script
    try:
        # Determine exec and bind paths: CLI overrides DB; if missing, error out
        p_params = ens.get('parameters', {})
        exec_path = args.exec_path if getattr(args, 'exec_path', None) else p_params.get('hmc_exec_path')
        bind_script = args.bind_script if getattr(args, 'bind_script', None) else p_params.get('hmc_bind_script')

        if not exec_path or not bind_script:
            print("ERROR: Missing required paths. Provide both --exec-path and --bind-script (they will be saved to the database).", file=sys.stderr)
            return 1

        # Save provided paths to ensemble parameters if passed via CLI
        if getattr(args, 'exec_path', None) or getattr(args, 'bind_script', None):
            try:
                conn = get_connection(args.db_file)
                cur = conn.cursor()
                if getattr(args, 'exec_path', None):
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO ensemble_parameters (ensemble_id, name, value)
                        VALUES (?, 'hmc_exec_path', ?)
                        """,
                        (ensemble_id, exec_path)
                    )
                if getattr(args, 'bind_script', None):
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO ensemble_parameters (ensemble_id, name, value)
                        VALUES (?, 'hmc_bind_script', ?)
                        """,
                        (ensemble_id, bind_script)
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"WARNING: Failed to save paths to DB: {e}", file=sys.stderr)

        generate_hmc_slurm_gpu(
            out_path=str(out_file),
            db_file=args.db_file,
            ensemble_id=ensemble_id,
            base_dir=args.base_dir,
            type_=root,
            ens_relpath=ens_rel,
            ens_name=ens_name,
            account=args.account,
            mode=args.mode,
            trajL=xml_dict['trajL'],
            lvl_sizes=xml_dict['lvl_sizes'],
            exec_path=exec_path,
            bind_script=bind_script,
            run_dir=getattr(args, 'run_dir', None),
            **job_params
        )
        print(f"Generated HMC script: {out_file}")
        
        # Save parameters to default params if requested
        if args.save_default_params:
            save_variant = args.save_params_as if args.save_params_as else args.mode
            success = save_operation_config(
                ens_dir, 'hmc', save_variant,
                xml_params=merged_xml_params,
                job_params=merged_job_params
            )
            if success:
                print(f"Saved parameters to {get_config_path(ens_dir)}: hmc.{save_variant}")
            else:
                print(f"Warning: Failed to save parameters to default params", file=sys.stderr)
        
        return 0
    except Exception as e:
        print(f"ERROR: Failed to generate HMC script: {e}", file=sys.stderr)
        return 1