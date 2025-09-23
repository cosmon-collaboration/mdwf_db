#!/usr/bin/env python3
"""
commands/query.py

Query command with two modes:
• with no -e/--ensemble: list all ensembles
• with   -e/--ensemble: show details + history for one
"""
import sys
import argparse
import re
from pathlib import Path
from MDWFutils.db import (
    list_ensembles,
    print_history,
    resolve_ensemble_identifier
)
from MDWFutils.cli.ensemble_utils import add_ensemble_argument


def extract_ensemble_params_from_path(directory_path):
    """
    Extract ensemble parameters from a directory path.
    Expected format: .../b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}/
    
    Returns:
        dict: Dictionary of parameter names and values, or empty dict if parsing fails
    """
    path_str = str(directory_path)
    
    # Define regex patterns for each parameter
    patterns = {
        'beta': r'b(\d+\.?\d*)',
        'b':    r'b(\d+\.?\d*)Ls',
        'Ls':   r'Ls(\d+)',
        'mc':   r'mc(\d+\.?\d*)',
        'ms':   r'ms(\d+\.?\d*)',
        'ml':   r'ml(\d+\.?\d*)',
        'L':    r'L(\d+)',
        'T':    r'T(\d+)',
    }
    
    params = {}
    for param_name, pattern in patterns.items():
        match = re.search(pattern, path_str)
        if match:
            params[param_name] = match.group(1)
    
    return params


def get_last_operation_and_user(db_file, ensemble_id):
    """
    Get the last operation and user from the database history for this ensemble.
    
    Args:
        db_file: Database file path
        ensemble_id: Ensemble ID
    
    Returns:
        tuple: (operation_type, username) or ("N/A", "N/A") if no operations
    """
    try:
        from MDWFutils.db import get_connection
        conn = get_connection(db_file)
        c = conn.cursor()
        
        c.execute("""
            SELECT operation_type, user FROM operations 
            WHERE ensemble_id = ? 
            ORDER BY creation_time DESC 
            LIMIT 1
        """, (ensemble_id,))
        
        row = c.fetchone()
        conn.close()
        
        if row:
            return row[0], row[1]
        else:
            return "N/A", "N/A"
    except Exception:
        return "N/A", "N/A"


def format_ensemble_list_spreadsheet(ensembles, db_file, sort_by_id=False):
    """
    Format ensembles in a spreadsheet-like format with columns.
    
    Args:
        ensembles: List of ensemble dictionaries
        db_file: Database file path
        sort_by_id: If True, sort by EID only. If False, sort by parameters.
    
    Returns:
        str: Formatted spreadsheet output
    """
    if not ensembles:
        return "No ensembles found"
    
    # Column headers in the specified order - EID first as row labels
    headers = ['EID', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'LAST_OP', 'LAST_USER']
    
    # Extract and prepare data for each ensemble
    ensemble_data = []
    for ens in ensembles:
        params = extract_ensemble_params_from_path(ens['directory'])
        last_op, last_user = get_last_operation_and_user(db_file, ens['id'])
        
        # Create row with all parameters, using "N/A" for missing values
        row = {
            'EID': str(ens['id']),
            'beta': params.get('beta', 'N/A'),
            'b': params.get('b', 'N/A'),
            'Ls': params.get('Ls', 'N/A'),
            'mc': params.get('mc', 'N/A'),
            'ms': params.get('ms', 'N/A'),
            'ml': params.get('ml', 'N/A'),
            'L': params.get('L', 'N/A'),
            'T': params.get('T', 'N/A'),
            'LAST_OP': last_op,
            'LAST_USER': last_user
        }
        ensemble_data.append(row)
    
    # Sort ensembles based on the sort_by_id flag
    if sort_by_id:
        # Sort by EID only (numerical order)
        ensemble_data.sort(key=lambda row: int(row['EID']))
    else:
        # Sort ensembles numerically/alphabetically by parameters (excluding EID from sorting)
        def sort_key(row):
            sort_values = []
            for header in headers:
                if header == 'EID':
                    continue  # Skip EID for sorting - it's just a row label
                value = row[header]
                if header in ['beta', 'b', 'mc', 'ms', 'ml'] and value != 'N/A':
                    try:
                        sort_values.append(float(value))
                    except ValueError:
                        sort_values.append(value)
                elif header in ['Ls', 'L', 'T'] and value != 'N/A':
                    try:
                        sort_values.append(int(value))
                    except ValueError:
                        sort_values.append(value)
                else:
                    sort_values.append(value)
            return tuple(sort_values)
        
        ensemble_data.sort(key=sort_key)
    
    # Calculate column widths
    col_widths = {}
    for header in headers:
        max_width = len(header)
        for row in ensemble_data:
            max_width = max(max_width, len(str(row[header])))
        col_widths[header] = max_width
    
    # Build the output
    lines = []
    
    # Header row
    header_line = "  ".join([f"{header:<{col_widths[header]}}" for header in headers])
    lines.append(header_line)
    
    # Separator line
    separator = "  ".join(["-" * col_widths[header] for header in headers])
    lines.append(separator)
    
    # Data rows
    for row in ensemble_data:
        data_line = "  ".join([f"{str(row[header]):<{col_widths[header]}}" for header in headers])
        # Find the ensemble to get its status
        ensemble_id = int(row['EID'])
        ensemble = next((ens for ens in ensembles if ens['id'] == ensemble_id), None)
        if ensemble:
            status = f"({ensemble['status']})"
            data_line += f"  {status}"
        lines.append(data_line)
    
    return "\n".join(lines)


def format_ensemble_list_entry(ens, detailed=False):
    """
    Format a single ensemble for list display.
    
    Args:
        ens: Ensemble dictionary from database
        detailed: Whether to show detailed information
    
    Returns:
        str: Formatted ensemble entry
    """
    # Extract parameters from directory path
    params = extract_ensemble_params_from_path(ens['directory'])
    
    # Create the main line with ID, status, and parameters
    status_str = f"({ens['status']})"
    
    if params:
        # Order parameters consistently: beta, b, Ls, mc, ms, ml, L, T
        param_order = ['beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T']
        param_str = ' '.join([f"{param}={params.get(param, '?')}" for param in param_order if param in params])
        main_line = f"[{ens['id']}] {status_str} {param_str}"
    else:
        # Fallback to directory name if parameter extraction fails
        dir_name = Path(ens['directory']).name
        main_line = f"[{ens['id']}] {status_str} {dir_name}"
    
    lines = [main_line]
    
    if detailed:
        # Add detailed information
        if ens.get('description'):
            lines.append(f"    Description: {ens['description']}")
        if 'operation_count' in ens:
            lines.append(f"    Operations: {ens['operation_count']}")
        if ens.get('parameters'):
            db_params = ', '.join([f"{k}={v}" for k, v in sorted(ens['parameters'].items())])
            lines.append(f"    DB Parameters: {db_params}")
    
    return '\n'.join(lines)


def register(subparsers):
    p = subparsers.add_parser(
        'query',
        help='List ensembles or show detailed info for one ensemble',
        description="""
Query the MDWF database for ensemble information.

TWO MODES:

1. List mode (no --ensemble specified):
   Shows a spreadsheet-like table of all ensembles with columns:
   EID > beta > b > Ls > mc > ms > ml > L > T > LAST_OP > LAST_USER
   
   Sorting options:
   • Default: Ensembles are sorted numerically/alphabetically by parameters (EID is just a row label)
   • --sort-by-id: Ensembles are sorted by EID in numerical order

2. Detail mode (with --ensemble specified):
   Shows complete information for one ensemble:
   - All physics parameters (beta, masses, lattice dimensions)
   - Full operation history with timestamps and parameters
   - Job status and configuration ranges

FLEXIBLE ENSEMBLE IDENTIFICATION:
The --ensemble parameter accepts multiple formats:
  • Ensemble ID: -e 1
  • Relative path: -e ./TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
  • Absolute path: -e /full/path/to/ensemble
  • Current directory: -e . (when run from within ensemble directory)

EXAMPLES:
  mdwf_db query                    # List all ensembles in spreadsheet format (sorted by parameters)
  mdwf_db query --sort-by-id       # List all ensembles sorted by EID
  mdwf_db query --detailed         # List all with descriptions and operation counts
  mdwf_db query -e 1               # Show ensemble 1 details
  mdwf_db query -e .               # Show current ensemble (when in ensemble dir)
  mdwf_db query -e 1 --dir         # Show only the directory path for ensemble 1
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        '-e', '--ensemble',
        help='Show detailed information for this ensemble (ID, directory path, or "." for current directory)'
    )
    p.add_argument(
        '--detailed',
        action='store_true',
        help='In list mode, show descriptions and operation counts for each ensemble'
    )
    p.add_argument(
        '--sort-by-id',
        action='store_true',
        help='In list mode, sort ensembles by EID instead of by parameters'
    )
    p.add_argument(
        '--dir',
        action='store_true',
        help='Show only the directory path (only works with --ensemble)'
    )
    p.set_defaults(func=do_query)


def _fetch_operations_summary(db_file, ensemble_id):
    """
    Fetch a compact summary of operations for an ensemble.
    Returns a list of dict rows with selected fields.
    """
    try:
        from MDWFutils.db import get_connection
        conn = get_connection(db_file)
        c = conn.cursor()

        c.execute(
            """
            SELECT id, operation_type, status, update_time, user
              FROM operations
             WHERE ensemble_id = ?
             ORDER BY id
            """,
            (ensemble_id,)
        )
        ops = c.fetchall()

        rows = []
        for oid, op_type, status, utime, user in ops:
            # Fetch a few common parameters
            pcur = conn.cursor()
            pcur.execute(
                """
                SELECT name, value FROM operation_parameters
                 WHERE operation_id=?
            """,
                (oid,)
            )
            params = {name: value for name, value in pcur.fetchall()}

            # Build compact fields
            cfg_start = params.get('config_start') or params.get('SC')
            cfg_end   = params.get('config_end') or params.get('EC')
            cfg_inc   = params.get('config_increment') or params.get('IC')
            rng = ""
            if cfg_start is not None and cfg_end is not None:
                rng = f"{cfg_start}-{cfg_end}"
                if cfg_inc is not None:
                    rng += f"({cfg_inc})"

            row = {
                'ID': str(oid),
                'TYPE': op_type,
                'STATUS': status,
                'UPDATED': utime or '',
                'USER': user or '',
                'RANGE': rng,
                'JOB': params.get('slurm_job', ''),
                'EXIT': params.get('exit_code', ''),
            }
            rows.append(row)

        conn.close()
        return rows
    except Exception:
        return []


def _format_operations_table(rows):
    if not rows:
        return "No operations recorded"
    headers = ['ID', 'TYPE', 'STATUS', 'UPDATED', 'USER', 'RANGE', 'JOB', 'EXIT']
    # compute widths
    widths = {h: len(h) for h in headers}
    for r in rows:
        for h in headers:
            widths[h] = max(widths[h], len(str(r.get(h, ''))))
    # build lines
    header = "  ".join(f"{h:<{widths[h]}}" for h in headers)
    sep    = "  ".join("-" * widths[h] for h in headers)
    lines  = [header, sep]
    for r in rows:
        line = "  ".join(f"{str(r.get(h, '')):<{widths[h]}}" for h in headers)
        lines.append(line)
    return "\n".join(lines)


def do_query(args):
    if not args.ensemble:
        # 1. List mode (no --ensemble):
        #    Show all ensembles in spreadsheet format
        ensembles = list_ensembles(args.db_file, detailed=args.detailed)
        if not ensembles:
            print("No ensembles found")
            return 0

        if args.detailed:
            # Use the same sorting logic as spreadsheet mode for consistency
            if args.sort_by_id:
                # Sort by EID only (numerical order)
                ensembles.sort(key=lambda ens: ens['id'])
            else:
                # Sort ensembles numerically/alphabetically by parameters
                def sort_key(ens):
                    params = extract_ensemble_params_from_path(ens['directory'])
                    sort_values = []
                    param_order = ['beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T']
                    for param in param_order:
                        value = params.get(param, 'N/A')
                        if param in ['beta', 'b', 'mc', 'ms', 'ml'] and value != 'N/A':
                            try:
                                sort_values.append(float(value))
                            except ValueError:
                                sort_values.append(value)
                        elif param in ['Ls', 'L', 'T'] and value != 'N/A':
                            try:
                                sort_values.append(int(value))
                            except ValueError:
                                sort_values.append(value)
                        else:
                            sort_values.append(value)
                    return tuple(sort_values)
                
                ensembles.sort(key=sort_key)
            
            # Now display in the sorted order
            for ens in ensembles:
                formatted_entry = format_ensemble_list_entry(ens, detailed=True)
                print(formatted_entry)
                print()  # blank line between detailed entries
        else:
            # Use the new spreadsheet format
            spreadsheet_output = format_ensemble_list_spreadsheet(ensembles, args.db_file, args.sort_by_id)
            print(spreadsheet_output)

    else:
        # 2. Detail mode (with --ensemble)
        ensemble_id, ens = resolve_ensemble_identifier(args.db_file, args.ensemble)
        if ensemble_id is None:
            print(f"ERROR: Ensemble not found: {args.ensemble}")
            return 1

        # If --dir is specified, show only the directory path
        if args.dir:
            print(ens['directory'])
            return 0

        # Print ensemble details (compact header)
        print(f"ID        = {ens['id']}")
        print(f"Directory = {ens['directory']}")
        print(f"Status    = {ens['status']}")
        if ens['description']:
            print(f"Description = {ens['description']}")

        if args.detailed:
            # Full detailed output (existing behavior)
            if ens['parameters']:
                print("Parameters:")
                for k, v in sorted(ens['parameters'].items()):
                    print(f"    {k} = {v}")
            print("\n=== Operation history ===")
            print_history(args.db_file, ensemble_id)
        else:
            # Show important paths (exec/bind) even in compact view
            params = ens.get('parameters', {}) or {}
            exec_path = params.get('hmc_exec_path')
            bind_script = params.get('hmc_bind_script')
            if exec_path or bind_script:
                print("HMC paths:")
                if exec_path:
                    print(f"  hmc_exec_path   = {exec_path}")
                if bind_script:
                    print(f"  hmc_bind_script = {bind_script}")

            # Compact operations table
            rows = _fetch_operations_summary(args.db_file, ensemble_id)
            print("\nOperations:")
            print(_format_operations_table(rows))

    return 0