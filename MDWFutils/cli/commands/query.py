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
    resolve_ensemble_identifier,
    get_configuration_range,
)
from MDWFutils.cli.ensemble_utils import add_ensemble_argument


def extract_ensemble_params_from_path(directory_path):
    """
    Robustly extract parameters from an ensemble directory path by parsing
    only the expected segments after TUNING/ or ENSEMBLES/.
    Expected tail: b{beta}/b{b}Ls{Ls}/mc{mc}/ms{ms}/ml{ml}/L{L}/T{T}
    """
    p = Path(str(directory_path))
    parts = list(p.parts)
    params = {}
    try:
        # Find anchor (TUNING or ENSEMBLES)
        anchor_idx = None
        for i, seg in enumerate(parts):
            if seg in ('TUNING', 'ENSEMBLES'):
                anchor_idx = i
                break
        if anchor_idx is None:
            # Fallback: assume the last 8 segments are the parameter segments
            tail = parts[-8:]
        else:
            tail = parts[anchor_idx+1:anchor_idx+1+7]
        if len(tail) < 7:
            return params
        beta_s, bLs_s, mc_s, ms_s, ml_s, L_s, T_s = tail[:7]
        if beta_s.startswith('b'):
            params['beta'] = beta_s[1:]
        if bLs_s.startswith('b') and 'Ls' in bLs_s:
            try:
                b_part, ls_part = bLs_s.split('Ls', 1)
                params['b'] = b_part[1:]
                params['Ls'] = ls_part
            except ValueError:
                pass
        if mc_s.startswith('mc'):
            params['mc'] = mc_s[2:]
        if ms_s.startswith('ms'):
            params['ms'] = ms_s[2:]
        if ml_s.startswith('ml'):
            params['ml'] = ml_s[2:]
        if L_s.startswith('L'):
            params['L'] = L_s[1:]
        if T_s.startswith('T'):
            params['T'] = T_s[1:]
    except Exception:
        return {}
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


def get_nicknames_map(db_file):
    """
    Return a mapping {ensemble_id: nickname} for all ensembles that have one.
    """
    try:
        from MDWFutils.db import get_connection
        conn = get_connection(db_file)
        c = conn.cursor()
        c.execute(
            """
            SELECT ensemble_id, value FROM ensemble_parameters
             WHERE name='nickname'
            """
        )
        rows = c.fetchall()
        conn.close()
        return {eid: nick for eid, nick in rows if nick is not None}
    except Exception:
        return {}


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
    headers = ['EID', 'NICK', 'beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T', 'N_CFG', 'LAST_OP', 'LAST_USER']
    
    # Extract and prepare data for each ensemble
    ensemble_data = []
    nick_map = get_nicknames_map(db_file)
    for ens in ensembles:
        params = extract_ensemble_params_from_path(ens['directory'])
        last_op, last_user = get_last_operation_and_user(db_file, ens['id'])
        # Compute number of configurations if stored (prefer cfg_total), otherwise derive
        n_cfg_val = ''
        try:
            from MDWFutils.db import get_configuration_range
            cfg = get_configuration_range(db_file, ens['id'])
            if cfg:
                total = cfg.get('total')
                if total is not None:
                    n_cfg_val = str(total)
                else:
                    f = cfg.get('first')
                    l = cfg.get('last')
                    inc = cfg.get('increment')
                    if f is not None and l is not None and inc is not None:
                        try:
                            f_i = int(f); l_i = int(l); inc_i = int(inc)
                            if inc_i > 0 and l_i >= f_i:
                                n_cfg_val = str(((l_i - f_i) // inc_i) + 1)
                        except Exception:
                            pass
        except Exception:
            pass
        
        # Create row with all parameters, using "N/A" for missing values
        row = {
            'EID': str(ens['id']),
            'NICK': nick_map.get(ens['id'], ''),
            'beta': params.get('beta', 'N/A'),
            'b': params.get('b', 'N/A'),
            'Ls': params.get('Ls', 'N/A'),
            'mc': params.get('mc', 'N/A'),
            'ms': params.get('ms', 'N/A'),
            'ml': params.get('ml', 'N/A'),
            'L': params.get('L', 'N/A'),
            'T': params.get('T', 'N/A'),
            'N_CFG': n_cfg_val,
            'LAST_OP': last_op,
            'LAST_USER': last_user
        }
        ensemble_data.append(row)
    
    # Sort ensembles based on the sort_by_id flag
    if sort_by_id:
        # Sort by EID only (numerical order)
        ensemble_data.sort(key=lambda row: int(row['EID']))
    else:
        # Sort ensembles numerically/alphabetically by parameters only (exclude EID/NICK/last columns)
        def sort_key(row):
            param_headers = ['beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T']
            sort_values = []
            for header in param_headers:
                value = row.get(header, 'N/A')
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
    
    # Create the main line with ID, status, optional nickname, and parameters
    status_str = f"({ens['status']})"
    nickname = None
    # Prefer nickname from parameters if present (in detailed mode it's included)
    if ens.get('parameters') and isinstance(ens['parameters'], dict):
        nickname = ens['parameters'].get('nickname')
    if not nickname:
        # Try to pull from directory parsing (not applicable), so leave None
        nickname = None
    
    if params:
        # Order parameters consistently: beta, b, Ls, mc, ms, ml, L, T
        param_order = ['beta', 'b', 'Ls', 'mc', 'ms', 'ml', 'L', 'T']
        param_str = ' '.join([f"{param}={params.get(param, '?')}" for param in param_order if param in params])
        if nickname:
            main_line = f"[{ens['id']}] {status_str} nick={nickname} {param_str}"
        else:
            main_line = f"[{ens['id']}] {status_str} {param_str}"
    else:
        # Fallback to directory name if parameter extraction fails
        dir_name = Path(ens['directory']).name
        if nickname:
            main_line = f"[{ens['id']}] {status_str} nick={nickname} {dir_name}"
        else:
            main_line = f"[{ens['id']}] {status_str} {dir_name}"
    
    lines = [main_line]
    
    if detailed:
        # Add detailed information
        if nickname:
            lines.append(f"    Nickname: {nickname}")
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
        # Show nickname explicitly if present
        params = ens.get('parameters', {}) or {}
        nickname = params.get('nickname')
        if nickname:
            print(f"Nickname  = {nickname}")
        # Show configuration range if stored
        try:
            cfg = get_configuration_range(args.db_file, ensemble_id)
            if cfg:
                first = cfg.get('first')
                last = cfg.get('last')
                inc = cfg.get('increment')
                total = cfg.get('total')
                parts = []
                if first is not None and last is not None:
                    parts.append(f"range: {first}-{last}")
                if inc is not None:
                    parts.append(f"step: {inc}")
                if total is not None:
                    parts.append(f"total: {total}")
                if parts:
                    print("Config    = " + ", ".join(parts))
        except Exception:
            pass

        if args.detailed:
            # Full detailed output (existing behavior)
            if ens['parameters']:
                print("Parameters:")
                for k, v in sorted(ens['parameters'].items()):
                    print(f"    {k} = {v}")
            # Data summary (measurements)
            try:
                params = ens.get('parameters', {}) or {}
                def fmt_summary(prefix: str, count_key: str) -> str:
                    total = params.get(f"{prefix}_total") or params.get(count_key)
                    parts = []
                    first = params.get(f"{prefix}_first")
                    last  = params.get(f"{prefix}_last")
                    inc   = params.get(f"{prefix}_increment")
                    if first is not None and last is not None and str(first) != '' and str(last) != '':
                        rng = f"range: {first}-{last}"
                        if inc:
                            rng += f", step: {inc}"
                        parts.append(rng)
                    if total is not None and str(total) != '':
                        parts.append(f"total: {total}")
                    return ", ".join(parts) if parts else ""

                print("\nScanned files:")
                smear_types = sorted({k[len('smear_'):-len('_total')] for k in params if isinstance(k, str) and k.startswith('smear_') and k.endswith('_total')})
                if smear_types:
                    for st in smear_types:
                        line = fmt_summary(f"smear_{st}", f"smear_{st}_count")
                        if line:
                            print(f"  {st} - {line}")
                else:
                    smear_line = fmt_summary('smear', 'smear_count')
                    print(f"  Smear - {smear_line if smear_line else (params.get('smear_count', 0))}")
                print(f"  t0 - {fmt_summary('t0','t0_count')}")
                print(f"  meson2pt - {fmt_summary('meson2pt','meson2pt_count')}")
                print(f"  mres - {fmt_summary('mres','mres_count')}")
                print(f"  Zv - {fmt_summary('zv','zv_count')}")
            except Exception:
                pass
            print("\n=== Operation history ===")
            print_history(args.db_file, ensemble_id)
        else:
            # Show important paths (exec/bind) even in compact view
            params = ens.get('parameters', {}) or {}
            exec_path = params.get('hmc_exec_path')
            bind_script_gpu = params.get('hmc_bind_script_gpu') or params.get('hmc_bind_script')
            bind_script_cpu = params.get('hmc_bind_script_cpu')
            print("HMC paths:")
            print(f"  hmc_exec_path   = {exec_path if exec_path else 'NOT SET'}")
            print(f"  hmc_bind_script_gpu = {bind_script_gpu if bind_script_gpu else 'NOT SET'}")
            print(f"  hmc_bind_script_cpu = {bind_script_cpu if bind_script_cpu else 'NOT SET'}")

            # Data summary (measurements) in compact view
            try:
                def fmt_summary(prefix: str, count_key: str) -> str:
                    total = params.get(f"{prefix}_total") or params.get(count_key)
                    parts = []
                    first = params.get(f"{prefix}_first")
                    last  = params.get(f"{prefix}_last")
                    inc   = params.get(f"{prefix}_increment")
                    if first is not None and last is not None and str(first) != '' and str(last) != '':
                        rng = f"range: {first}-{last}"
                        if inc:
                            rng += f", step: {inc}"
                        parts.append(rng)
                    if total is not None and str(total) != '':
                        parts.append(f"total: {total}")
                    return ", ".join(parts) if parts else ""

                print("\nScanned files:")
                smear_types = sorted({k[len('smear_'):-len('_total')] for k in params if isinstance(k, str) and k.startswith('smear_') and k.endswith('_total')})
                if smear_types:
                    for st in smear_types:
                        line = fmt_summary(f"smear_{st}", f"smear_{st}_count")
                        if line:
                            print(f"  {st} - {line}")
                else:
                    smear_line = fmt_summary('smear', 'smear_count')
                    print(f"  Smear - {smear_line if smear_line else (params.get('smear_count', 0))}")
                print(f"  t0 - {fmt_summary('t0','t0_count')}")
                print(f"  meson2pt - {fmt_summary('meson2pt','meson2pt_count')}")
                print(f"  mres - {fmt_summary('mres','mres_count')}")
                print(f"  Zv - {fmt_summary('zv','zv_count')}")
            except Exception:
                pass

            # Compact operations table
            rows = _fetch_operations_summary(args.db_file, ensemble_id)
            print("\nOperations:")
            print(_format_operations_table(rows))

    return 0