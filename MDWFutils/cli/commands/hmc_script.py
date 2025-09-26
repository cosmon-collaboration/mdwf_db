#!/usr/bin/env python3
"""
commands/hmc_script.py

Generate HMC XML and SLURM script for gauge configuration generation.
"""
import argparse
import os
import sys
import re
from pathlib import Path
from MDWFutils.db import get_ensemble_details, resolve_ensemble_identifier, get_connection
from MDWFutils.jobs.hmc import generate_hmc_parameters, generate_hmc_slurm_gpu, generate_hmc_slurm_cpu
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
    subp = p.add_subparsers(dest='hmc_target', required=True)

    def add_common_args(pp: argparse.ArgumentParser):
        # Flexible identifier (preferred)
        pp.add_argument('-e', '--ensemble', required=False,
                        help='Ensemble ID, directory path, or "." for current directory')
        # Legacy integer-only option for backward compatibility
        pp.add_argument('--ensemble-id', dest='ensemble_id', type=int, required=False,
                        help='[DEPRECATED] Ensemble ID (use -e/--ensemble for flexible ID or path)')
        pp.add_argument('-a', '--account', required=True,
                        help='SLURM account name (e.g., m2986)')
        pp.add_argument('-m', '--mode', required=True,
                        choices=['tepid', 'continue', 'reseed'],
                        help='HMC run mode: tepid (new), continue (existing), or reseed (new seed)')
        pp.add_argument('--base-dir', default='.',
                        help='Root directory containing TUNING/ and ENSEMBLES/ (default: current directory)')
        pp.add_argument('-x', '--xml-params', required=True,
                        help='Space-separated key=val pairs for HMC XML parameters. Required: trajL, lvl_sizes')
        pp.add_argument('-j', '--job-params', default='',
                        help='Space-separated key=val pairs for SLURM job parameters. Required: cfg_max')
        pp.add_argument('-o', '--output-file',
                        help='Output SBATCH script path (auto-generated if not specified)')
        pp.add_argument('--exec-path',
                        help='Path to HMC executable (saved to DB as hmc_exec_path)')
        pp.add_argument('--bind-script',
                        help='Path to binding script. GPU: saved as hmc_bind_script_gpu, CPU: hmc_bind_script_cpu')
        pp.add_argument('--run-dir',
                        help='Directory to run the job from (must contain a full copy of the ensemble directory)')
        pp.add_argument('--use-default-params', action='store_true',
                        help='Load parameters from ensemble default parameter file (mdwf_default_params.yaml)')
        pp.add_argument('--params-variant',
                        help='Specify which parameter variant to use (overrides default lookup). For HMC: tepid, continue, reseed')
        pp.add_argument('--save-default-params', action='store_true',
                        help='Save current command parameters to default parameter file for later reuse')
        pp.add_argument('--save-params-as',
                        help='Save current parameters under specific variant name (default: use current mode)')

    pgpu = subp.add_parser('gpu', help='Generate GPU HMC SLURM script')
    add_common_args(pgpu)
    pgpu.set_defaults(func=do_hmc_script_gpu)

    pcpu = subp.add_parser('cpu', help='Generate CPU HMC SLURM script')
    add_common_args(pcpu)
    pcpu.set_defaults(func=do_hmc_script_cpu)

def do_hmc_script_gpu(args):
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

    # Normalize SBATCH-style keys with dashes to Python kwargs (underscores)
    # e.g., cpus-per-task -> cpus_per_task, ntasks-per-node -> ntasks_per_node
    if job_dict:
        normalized = {}
        for k, v in job_dict.items():
            k2 = k.replace('-', '_')
            normalized[k2] = v
        job_dict = normalized
        # Map uppercase env-style key to our normalized key
        if 'OMP_NUM_THREADS' in job_dict:
            job_dict['omp_num_threads'] = job_dict.pop('OMP_NUM_THREADS')
    
    # Remove deprecated cfg_max if provided (no longer used; controlled by XML Trajectories)
    if 'cfg_max' in job_dict:
        job_dict.pop('cfg_max', None)
    
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
    if 'Trajectories' not in xml_dict:
        missing_params.append('Trajectories')
    
    if missing_params:
        if args.use_default_params:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}. Add them to your default parameter file or use -x '{' '.join(missing_params)}'", file=sys.stderr)
        else:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}", file=sys.stderr)
        return 1
    
    # Get base directory and compute relative paths
    # Auto-detect project root containing TUNING/ or ENSEMBLES/ in path if possible
    base = Path(args.base_dir).resolve()
    parts = list(ens_dir.parts)
    root_idx = None
    for i, part in enumerate(parts):
        if part in ('TUNING', 'ENSEMBLES'):
            root_idx = i
            break
    if root_idx is not None:
        detected_base = Path(*parts[:root_idx]) if root_idx > 0 else Path('/')
        if str(detected_base):
            base = detected_base.resolve()
    
    try:
        rel = ens_dir.relative_to(base)
    except ValueError:
        print(f"ERROR: {ens_dir} is not under base-dir {base}", file=sys.stderr)
        return 1
    
    root = rel.parts[0]  # "TUNING" or "ENSEMBLES"
    ens_rel = str(rel)   # e.g. "TUNING/b6.0/.../T32"
    ens_name = ens_rel.replace('TUNING/', '').replace('ENSEMBLES/', '').replace('/', '_')
    
    # Ensure slurm folder & output path (prefer run_dir when provided)
    work_root = Path(getattr(args, 'run_dir', '')).resolve() if getattr(args, 'run_dir', None) else ens_dir
    slurm_dir = work_root / 'slurm'
    slurm_dir.mkdir(parents=True, exist_ok=True)
    out_file = args.output_file or slurm_dir / f"hmc_gpu_{ensemble_id}_{args.mode}.sbatch"
    
    # Generate HMC XML parameters into run directory 'cnfg' with computed StartTrajectory
    try:
        work_root = Path(getattr(args, 'run_dir', '')).resolve() if getattr(args, 'run_dir', None) else ens_dir
        cnfg_dir = work_root / 'cnfg'
        cnfg_dir.mkdir(parents=True, exist_ok=True)

        # Compute current start by scanning existing 'lat*' files in cnfg
        start = 0
        numbers = []
        for f in cnfg_dir.iterdir():
            if not f.is_file():
                continue
            if 'lat' not in f.name:
                continue
            m = re.findall(r"(\d+)", f.name)
            if m:
                try:
                    numbers.append(int(m[-1]))
                except ValueError:
                    pass
        if numbers:
            start = max(numbers)

        # Force StartTrajectory to detected start
        xml_dict['StartTrajectory'] = str(start)

        generate_hmc_parameters(str(cnfg_dir), mode=args.mode, **xml_dict)
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
        # 'mail_user': only include if provided via -j; no default
        'mpi': '2.1.1.2',
        'omp_num_threads': '16',
    }
    job_params.update(job_dict)
    
    # Set ntasks_per_node if not provided
    if 'ntasks_per_node' not in job_params:
        job_params['ntasks_per_node'] = job_params['cpus_per_task']
    
    # No automatic resubmission logic; user can resubmit manually if desired
    
    # Generate SLURM script
    try:
        # Determine exec and bind paths: CLI overrides DB; if missing, error out
        p_params = ens.get('parameters', {})
        exec_path = args.exec_path if getattr(args, 'exec_path', None) else p_params.get('hmc_exec_path')
        # Prefer GPU-specific key, fall back to legacy
        bind_script = args.bind_script if getattr(args, 'bind_script', None) else (p_params.get('hmc_bind_script_gpu') or p_params.get('hmc_bind_script'))

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
                        VALUES (?, 'hmc_bind_script_gpu', ?)
                        """,
                        (ensemble_id, bind_script)
                    )
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"WARNING: Failed to save paths to DB: {e}", file=sys.stderr)

        # Avoid passing omp_num_threads twice (explicit kw + **job_params)
        _omp_threads = job_params.pop('omp_num_threads', None)
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
            n_trajec=xml_dict['Trajectories'],
            omp_num_threads=_omp_threads,
            **job_params
        )
        print(f"Generated HMC GPU script: {out_file}")
        
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
        print(f"ERROR: Failed to generate HMC GPU script: {e}", file=sys.stderr)
        return 1


def do_hmc_script_cpu(args):
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

    # Normalize SBATCH-style keys with dashes to Python kwargs (underscores)
    if job_dict:
        normalized = {}
        for k, v in job_dict.items():
            k2 = k.replace('-', '_')
            normalized[k2] = v
        job_dict = normalized
        if 'OMP_NUM_THREADS' in job_dict:
            job_dict['omp_num_threads'] = job_dict.pop('OMP_NUM_THREADS')

    # Remove deprecated cfg_max if provided (no longer used; controlled by XML Trajectories)
    if 'cfg_max' in job_dict:
        job_dict.pop('cfg_max', None)

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
    if 'Trajectories' not in xml_dict:
        missing_params.append('Trajectories')

    if missing_params:
        if args.use_default_params:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}. Add them to your default parameter file or use -x '{' '.join(missing_params)}'", file=sys.stderr)
        else:
            print(f"ERROR: Required XML parameters missing: {', '.join(missing_params)}", file=sys.stderr)
        return 1

    # Get base directory and compute relative paths
    base = Path(args.base_dir).resolve()
    parts = list(ens_dir.parts)
    root_idx = None
    for i, part in enumerate(parts):
        if part in ('TUNING', 'ENSEMBLES'):
            root_idx = i
            break
    if root_idx is not None:
        detected_base = Path(*parts[:root_idx]) if root_idx > 0 else Path('/')
        if str(detected_base):
            base = detected_base.resolve()

    try:
        rel = ens_dir.relative_to(base)
    except ValueError:
        print(f"ERROR: {ens_dir} is not under base-dir {base}", file=sys.stderr)
        return 1

    root = rel.parts[0]
    ens_rel = str(rel)
    ens_name = ens_rel.replace('TUNING/', '').replace('ENSEMBLES/', '').replace('/', '_')

    # Ensure slurm folder & output path (prefer run_dir when provided)
    work_root = Path(getattr(args, 'run_dir', '')).resolve() if getattr(args, 'run_dir', None) else ens_dir
    slurm_dir = work_root / 'slurm'
    slurm_dir.mkdir(parents=True, exist_ok=True)
    out_file = args.output_file or slurm_dir / f"hmc_cpu_{ensemble_id}_{args.mode}.sbatch"

    # Generate HMC XML parameters into run directory 'cnfg' with computed StartTrajectory
    try:
        work_root = Path(getattr(args, 'run_dir', '')).resolve() if getattr(args, 'run_dir', None) else ens_dir
        cnfg_dir = work_root / 'cnfg'
        cnfg_dir.mkdir(parents=True, exist_ok=True)

        # Compute current start by scanning existing 'lat*' files in cnfg
        start = 0
        numbers = []
        for f in cnfg_dir.iterdir():
            if not f.is_file():
                continue
            if 'lat' not in f.name:
                continue
            m = re.findall(r"(\d+)", f.name)
            if m:
                try:
                    numbers.append(int(m[-1]))
                except ValueError:
                    pass
        if numbers:
            start = max(numbers)

        # Force StartTrajectory to detected start
        xml_dict['StartTrajectory'] = str(start)

        generate_hmc_parameters(str(cnfg_dir), mode=args.mode, **xml_dict)
    except Exception as e:
        print(f"ERROR: Failed to generate HMC XML: {e}", file=sys.stderr)
        return 1

    # CPU defaults
    job_params = {
        'constraint': 'cpu',
        'time_limit': '17:00:00',
        'cpus_per_task': '32',
        'nodes': '1',
        'queue': 'regular',
        'omp_num_threads': '4',
    }
    job_params.update(job_dict)

    if 'ntasks_per_node' not in job_params:
        job_params['ntasks_per_node'] = job_params['cpus_per_task']

    # Determine exec and bind paths
    p_params = ens.get('parameters', {})
    exec_path = args.exec_path if getattr(args, 'exec_path', None) else p_params.get('hmc_exec_path')
    # Prefer CPU-specific key, fall back to legacy
    bind_script = args.bind_script if getattr(args, 'bind_script', None) else (p_params.get('hmc_bind_script_cpu') or p_params.get('hmc_bind_script'))

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
                    VALUES (?, 'hmc_bind_script_cpu', ?)
                    """,
                    (ensemble_id, bind_script)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"WARNING: Failed to save paths to DB: {e}", file=sys.stderr)

    # Generate SLURM script (CPU)
    try:
        # Avoid passing omp_num_threads twice (explicit kw + **job_params)
        _omp_threads = job_params.pop('omp_num_threads', None)
        generate_hmc_slurm_cpu(
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
            n_trajec=xml_dict['Trajectories'],
            omp_num_threads=_omp_threads,
            **job_params
        )
        print(f"Generated HMC CPU script: {out_file}")
        
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
        print(f"ERROR: Failed to generate HMC CPU script: {e}", file=sys.stderr)
        return 1