#!/usr/bin/env python3
"""
commands/scan_configs.py

Scan all ensembles and infer configuration ranges from each cnfg/ directory.
Stores results in ensemble_parameters via set_configuration_range.
"""

import sys
import re
from pathlib import Path
from typing import Optional, List, Tuple, Dict

from MDWFutils.db import list_ensembles, set_configuration_range, get_ensemble_id_by_directory, set_ensemble_parameter


def _extract_numbers_from_cnfg(cnfg_dir: Path) -> List[int]:
    """
    Extract all numeric suffixes from files in cnfg_dir that contain 'lat'.
    Returns a sorted unique list of integers.
    """
    if not cnfg_dir.exists() or not cnfg_dir.is_dir():
        return []
    nums = []
    for p in cnfg_dir.iterdir():
        if not p.is_file():
            continue
        name = p.name
        if 'lat' not in name:
            continue
        m = re.findall(r"(\d+)", name)
        if not m:
            continue
        try:
            nums.append(int(m[-1]))
        except ValueError:
            continue
    return sorted(set(nums))


def _infer_increment(values: List[int]) -> Optional[int]:
    """
    Infer a constant increment if the sequence is arithmetic.
    Requires at least two values. Returns None if not consistent.
    """
    if len(values) < 2:
        return None
    inc = values[1] - values[0]
    if inc <= 0:
        return None
    for i in range(2, len(values)):
        if values[i] - values[i-1] != inc:
            return None
    return inc


def _extract_params_signature(ens_dir: Path) -> Optional[Tuple[str, str, str, str, str, str, str, str]]:
    """
    Extract a canonical signature (beta, b, Ls, mc, ms, ml, L, T) from an ensemble directory path.
    Returns None if the required segments cannot be parsed.
    """
    s = str(ens_dir)
    def find(pat: str) -> Optional[str]:
        m = re.search(pat, s)
        return m.group(1) if m else None
    beta = find(r"b(\d+\.?\d*)/b")
    b    = find(r"b(\d+\.?\d*)Ls")
    Ls   = find(r"Ls(\d+)")
    mc   = find(r"mc(\d+\.?\d*)")
    ms   = find(r"ms(\d+\.?\d*)")
    ml   = find(r"ml(\d+\.?\d*)")
    L    = find(r"L(\d+)/T")
    T    = find(r"T(\d+)")
    if None in (beta, b, Ls, mc, ms, ml, L, T):
        return None
    return (beta, b, Ls, mc, ms, ml, L, T)


def register(subparsers):
    p = subparsers.add_parser(
        'scan',
        help='Scan cnfg/ folders to store config ranges and optionally scan filesystem',
        description='Infers first, last, increment, and total configuration counts per ensemble.'
    )
    p.add_argument(
        '--scan-fs',
        action='store_true',
        help='Also scan filesystem under base dir for ensembles not in DB and report their ranges'
    )
    p.add_argument(
        '--base-dir',
        default=None,
        help='Base directory containing TUNING/ and ENSEMBLES/. Defaults to the directory of the DB file.'
    )
    p.set_defaults(func=do_scan_configs)


def do_scan_configs(args):
    ens_list = list_ensembles(args.db_file, detailed=True)
    updated = 0
    for ens in ens_list:
        ens_dir = Path(ens['directory'])
        cnfg_dir = ens_dir / 'cnfg'
        # Change detection: compute quick stats
        file_count = 0
        latest_mtime = 0
        if cnfg_dir.exists() and cnfg_dir.is_dir():
            for p in cnfg_dir.iterdir():
                if not p.is_file():
                    continue
                try:
                    st = p.stat()
                except OSError:
                    continue
                file_count += 1
                if st.st_mtime > latest_mtime:
                    latest_mtime = st.st_mtime

        # Compare with stored values to skip unchanged
        params = ens.get('parameters') or {}
        prev_count = None
        prev_mtime = None
        try:
            if 'cnfg_count' in params:
                prev_count = int(params['cnfg_count'])
            if 'cnfg_mtime' in params:
                prev_mtime = float(params['cnfg_mtime'])
        except Exception:
            prev_count = None
            prev_mtime = None

        if prev_count is not None and prev_mtime is not None:
            if prev_count == file_count and abs(prev_mtime - latest_mtime) < 1e-6:
                # No changes detected; skip detailed scan
                continue

        # Detailed scan only if changed or no prior record
        vals = _extract_numbers_from_cnfg(cnfg_dir)
        if not vals:
            continue
        first = vals[0]
        last = vals[-1]
        inc = _infer_increment(vals)
        total = len(vals)
        try:
            set_configuration_range(args.db_file, ens['id'], first=first, last=last, increment=inc, total=total)
            # Persist change-detection markers
            set_ensemble_parameter(args.db_file, ens['id'], 'cnfg_count', str(file_count))
            set_ensemble_parameter(args.db_file, ens['id'], 'cnfg_mtime', str(latest_mtime))
            updated += 1
        except Exception as e:
            print(f"WARNING: Failed to set config range for ensemble {ens['id']}: {e}", file=sys.stderr)

    # Optionally scan filesystem for ensembles not in DB
    fs_reported = 0
    if getattr(args, 'scan_fs', False):
        base_dir = Path(args.base_dir).resolve() if getattr(args, 'base_dir', None) else Path(args.db_file).resolve().parent
        # Build a set of canonical parameter signatures present in the DB to avoid double-counting
        db_signatures: Dict[Tuple[str,str,str,str,str,str,str,str], str] = {}
        for ens in ens_list:
            sig = _extract_params_signature(Path(ens['directory']))
            if sig:
                db_signatures[sig] = ens['directory']
        for root_name in ('TUNING', 'ENSEMBLES'):
            root = base_dir / root_name
            if not root.exists():
                continue
            for cnfg_dir in root.rglob('cnfg'):
                ens_dir = cnfg_dir.parent.resolve()
                eid = get_ensemble_id_by_directory(args.db_file, str(ens_dir))
                if eid is not None:
                    continue  # already handled via DB
                # If an ensemble with the same parameters exists in DB (e.g., promoted), skip FS-only report
                sig = _extract_params_signature(ens_dir)
                if sig and sig in db_signatures:
                    continue
                vals = _extract_numbers_from_cnfg(cnfg_dir)
                if not vals:
                    continue
                first = vals[0]
                last = vals[-1]
                inc = _infer_increment(vals)
                total = len(vals)
                parts = [f"range: {first}-{last}"]
                if inc is not None:
                    parts.append(f"step: {inc}")
                parts.append(f"total: {total}")
                print(f"FS-only ensemble: {ens_dir} -> " + ", ".join(parts))
                fs_reported += 1

    msg = f"Updated configuration ranges for {updated} ensemble(s)"
    if getattr(args, 'scan_fs', False):
        msg += f"; reported {fs_reported} filesystem ensemble(s) not in DB"
    print(msg)
    return 0


