#!/usr/bin/env python3
"""List ensembles or show details using the new backend."""

import argparse

from ..ensemble_utils import resolve_ensemble_from_args, get_backend_for_args


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show details for one ensemble',
        description="""
Query the MDWF database for ensemble information.

THREE MODES:

1. List mode (no --ensemble specified):
   Shows a spreadsheet-like table of all ensembles with columns:
   EID, NICK, beta, b, Ls, mc, ms, ml, L, T, N_CFG, LAST_OP, LAST_USER, STATUS
   
   Sorting options:
   • Default: Ensembles are sorted numerically by physics parameters
   • --sort-by-id: Ensembles are sorted by EID in numerical order

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble including physics parameters,
   configuration details, HMC paths, and operation history.

3. Operation detail mode (with --ensemble and --op specified):
   Shows complete information for a specific operation including all timing,
   SLURM details, execution context, chain information, and parameters.

EXAMPLES:
  mdwf_db query                    # List all ensembles (sorted by parameters)
  mdwf_db query --sort-by-id       # List all ensembles sorted by EID
  mdwf_db query -e 1               # Show ensemble 1 details
  mdwf_db query -e .               # Show current ensemble
  mdwf_db query -e 1 --dir         # Show only the directory path
  mdwf_db query -e 21 --op 147     # Show details for operation 147
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('-e', '--ensemble', help='Ensemble identifier for detail view')
    p.add_argument('--detailed', action='store_true', help='Show extended operation details in detail view')
    p.add_argument('--sort-by-id', action='store_true', help='Sort list output by ensemble ID')
    p.add_argument('--dir', action='store_true', help='Only print directory path in detail view')
    p.add_argument('--op', '--operation', type=int, metavar='OP_ID',
                   help='Show detailed information for a specific operation (requires -e)')
    p.set_defaults(func=do_query)


def _safe_float(val, default=999.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default=999):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def do_query(args):
    backend = get_backend_for_args(args)

    # Check if --op is used without -e
    if args.op and not args.ensemble:
        print("ERROR: --op requires -e/--ensemble")
        return 1

    if not args.ensemble:
        ensembles = backend.list_ensembles(detailed=True)
        if not ensembles:
            print('No ensembles found')
            return 0

        # Build table data
        rows = []
        for ens in ensembles:
            ensemble_id = ens.get('ensemble_id', '')
            physics = ens.get('physics', {})
            cfg = ens.get('configurations', {})
            
            # Calculate N_CFG if possible
            n_cfg = ''
            if cfg.get('total'):
                n_cfg = str(cfg['total'])
            elif cfg.get('first') is not None and cfg.get('last') is not None and cfg.get('increment'):
                try:
                    n_cfg = str((cfg['last'] - cfg['first']) // cfg['increment'] + 1)
                except:
                    pass
            
            # Get last operation info
            last_op = ''
            last_user = ''
            try:
                ops = backend.list_operations(ensemble_id)
                if ops:
                    # Get the most recent operation by update_time
                    latest = max(ops, key=lambda o: o.get('timing', {}).get('update_time') or '')
                    last_op = latest.get('operation_type', '')
                    last_user = latest.get('slurm', {}).get('user', '')
            except:
                pass
            
            row = {
                'EID': ensemble_id,
                'NICK': ens.get('nickname', ''),
                'beta': physics.get('beta', ''),
                'b': physics.get('b', ''),
                'Ls': physics.get('Ls', ''),
                'mc': physics.get('mc', ''),
                'ms': physics.get('ms', ''),
                'ml': physics.get('ml', ''),
                'L': physics.get('L', ''),
                'T': physics.get('T', ''),
                'N_CFG': n_cfg,
                'LAST_OP': last_op,
                'LAST_USER': last_user,
                'STATUS': ens.get('status', ''),
            }
            rows.append(row)

        # Sort ensembles
        if args.sort_by_id:
            rows.sort(key=lambda r: r['EID'])
        else:
            # Sort by physics parameters
            def sort_key(row):
                return (
                    _safe_float(row['beta']),
                    _safe_float(row['b']),
                    _safe_int(row['Ls']),
                    _safe_float(row['mc']),
                    _safe_float(row['ms']),
                    _safe_float(row['ml']),
                    _safe_int(row['L']),
                    _safe_int(row['T']),
                )
            rows.sort(key=sort_key)

        # Print table
        headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'LAST_OP', 'LAST_USER', 'STATUS']
        _print_table(headers, rows)
        return 0

    # Detail mode - show specific ensemble
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1

    if args.dir:
        print(ensemble['directory'])
        return 0

    # Operation detail mode - show specific operation
    if args.op:
        operation = backend.get_operation(ensemble_id, args.op)
        if not operation:
            print(f"ERROR: Operation {args.op} not found for ensemble {ensemble_id}")
            return 1
        
        _print_operation_details(operation)
        return 0

    # Print ensemble header
    print(f"ID        = {ensemble_id}")
    print(f"Directory = {ensemble['directory']}")
    print(f"Status    = {ensemble.get('status', 'UNKNOWN')}")
    if ensemble.get('nickname'):
        print(f"Nickname  = {ensemble['nickname']}")
    if ensemble.get('description'):
        print(f"Description = {ensemble['description']}")

    # Physics parameters
    physics = ensemble.get('physics', {})
    if physics:
        print('\nPhysics parameters:')
        for key in ['beta', 'b', 'Ls', 'ml', 'ms', 'mc', 'L', 'T']:
            if key in physics:
                print(f"  {key} = {physics[key]}")

    # Configuration range
    cfg = ensemble.get('configurations', {})
    if cfg and any(cfg.values()):
        parts = []
        if cfg.get('first') is not None and cfg.get('last') is not None:
            parts.append(f"range: {cfg['first']}-{cfg['last']}")
        if cfg.get('increment'):
            parts.append(f"step: {cfg['increment']}")
        if cfg.get('total'):
            parts.append(f"total: {cfg['total']}")
        if parts:
            print("Config    = " + ", ".join(parts))

    # HMC paths
    hmc_paths = ensemble.get('hmc_paths', {})
    if hmc_paths and any(hmc_paths.values()):
        print("\nHMC paths:")
        if hmc_paths.get('exec_path'):
            print(f"  exec_path       = {hmc_paths['exec_path']}")
        if hmc_paths.get('bind_script_gpu'):
            print(f"  bind_script_gpu = {hmc_paths['bind_script_gpu']}")
        if hmc_paths.get('bind_script_cpu'):
            print(f"  bind_script_cpu = {hmc_paths['bind_script_cpu']}")

    # Operations summary table
    print('\nOperations:')
    ops = backend.list_operations(ensemble_id)
    if not ops:
        print('  (none)')
    else:
        op_rows = []
        for op in ops:
            timing = op.get('timing', {})
            slurm = op.get('slurm', {})
            execution = op.get('execution', {})
            
            # Build config range string
            cfg_range = ''
            if execution.get('config_start') is not None and execution.get('config_end') is not None:
                cfg_range = f"{execution['config_start']}-{execution['config_end']}"
                if execution.get('config_increment'):
                    cfg_range += f"({execution['config_increment']})"
            
            op_rows.append({
                'ID': op.get('operation_id', ''),
                'TYPE': op.get('operation_type', ''),
                'STATUS': op.get('status', ''),
                'UPDATED': str(timing.get('update_time', ''))[:19] if timing.get('update_time') else '',
                'USER': slurm.get('user', ''),
                'RANGE': cfg_range,
                'JOB': slurm.get('job_id', ''),
                'EXIT': slurm.get('exit_code', ''),
            })
        
        op_headers = ['ID', 'TYPE', 'STATUS', 'UPDATED', 'USER', 'RANGE', 'JOB', 'EXIT']
        _print_table(op_headers, op_rows)

    return 0


def _print_table(headers, rows):
    """Print a formatted table with proper column alignment."""
    if isinstance(rows[0], dict):
        # rows are dictionaries
        widths = {h: len(str(h)) for h in headers}
        for row in rows:
            for h in headers:
                widths[h] = max(widths[h], len(str(row.get(h, ''))))
        
        header_line = "  ".join(str(h).ljust(widths[h]) for h in headers)
        print(header_line)
        print("  ".join("-" * widths[h] for h in headers))
        for row in rows:
            print("  ".join(str(row.get(h, '')).ljust(widths[h]) for h in headers))
    else:
        # rows are lists
        widths = [len(str(h)) for h in headers]
        for row in rows:
            for idx, value in enumerate(row):
                widths[idx] = max(widths[idx], len(str(value)))
        
        header_line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
        print(header_line)
        print("  ".join("-" * w for w in widths))
        for row in rows:
            print("  ".join(str(row[i]).ljust(widths[i]) for i in range(len(headers))))


def _print_operation_details(operation):
    """Print detailed information about a single operation."""
    print(f"Operation ID: {operation.get('operation_id')}")
    print(f"Ensemble ID:  {operation.get('ensemble_id')}")
    print(f"Type:         {operation.get('operation_type')}")
    print(f"Status:       {operation.get('status')}")
    print(f"Directory:    {operation.get('ensemble_directory', 'N/A')}")
    
    # Timing section
    timing = operation.get('timing', {})
    if timing:
        print("\nTiming:")
        if timing.get('creation_time'):
            print(f"  Created:  {str(timing['creation_time'])[:19]}")
        if timing.get('start_time'):
            print(f"  Started:  {str(timing['start_time'])[:19]}")
        if timing.get('update_time'):
            print(f"  Updated:  {str(timing['update_time'])[:19]}")
        if timing.get('end_time'):
            print(f"  Ended:    {str(timing['end_time'])[:19]}")
        if timing.get('runtime_seconds') is not None:
            runtime = timing['runtime_seconds']
            hours = runtime // 3600
            minutes = (runtime % 3600) // 60
            seconds = runtime % 60
            print(f"  Runtime:  {runtime}s ({hours}h {minutes}m {seconds}s)")
    
    # SLURM section
    slurm = operation.get('slurm', {})
    if slurm and any(slurm.values()):
        print("\nSLURM:")
        if slurm.get('job_id'):
            print(f"  Job ID:      {slurm['job_id']}")
        if slurm.get('user'):
            print(f"  User:        {slurm['user']}")
        if slurm.get('host'):
            print(f"  Host:        {slurm['host']}")
        if slurm.get('exit_code') is not None:
            print(f"  Exit code:   {slurm['exit_code']}")
        if slurm.get('slurm_status'):
            print(f"  SLURM status: {slurm['slurm_status']}")
        if slurm.get('batch_script'):
            print(f"  Batch script: {slurm['batch_script']}")
        if slurm.get('output_log'):
            print(f"  Output log:   {slurm['output_log']}")
        if slurm.get('error_log'):
            print(f"  Error log:    {slurm['error_log']}")
    
    # Execution section
    execution = operation.get('execution', {})
    if execution and any(execution.values()):
        print("\nExecution:")
        if execution.get('run_dir'):
            print(f"  Run dir:    {execution['run_dir']}")
        if execution.get('config_start') is not None:
            print(f"  Config start: {execution['config_start']}")
        if execution.get('config_end') is not None:
            print(f"  Config end:   {execution['config_end']}")
        if execution.get('config_increment') is not None:
            print(f"  Config step:  {execution['config_increment']}")
    
    # Chain section
    chain = operation.get('chain', {})
    if chain and (chain.get('is_chain_member') or chain.get('parent_operation_id')):
        print("\nChain:")
        print(f"  Is chain member: {chain.get('is_chain_member', False)}")
        if chain.get('parent_operation_id'):
            print(f"  Parent op ID:    {chain['parent_operation_id']}")
        print(f"  Attempt number:  {chain.get('attempt_number', 1)}")
    
    # Additional parameters
    params = operation.get('params', {})
    if params:
        print("\nAdditional parameters:")
        for key, value in sorted(params.items()):
            print(f"  {key} = {value}")
