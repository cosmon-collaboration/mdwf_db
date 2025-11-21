#!/usr/bin/env python3
"""
commands/scan_configs.py

Scan ensemble directories to detect configuration ranges and measurement files.
Stores results in ensemble_parameters for tracking and monitoring.
"""

import sys
import re
import stat
import grp
import pwd
import os
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Set

from MDWFutils.db import list_ensembles, set_configuration_range, get_ensemble_id_by_directory, set_ensemble_parameter


def _check_file_permissions(file_path: Path, is_config_file: bool = False) -> Tuple[List[str], Optional[str]]:
    """
    Check file permissions and ownership.
    
    Args:
        file_path: Path to the file to check
        is_config_file: True if this is a configuration file (lat.* files)
    
    Returns:
        Tuple of (list of permission issues, username of file owner or None)
    """
    issues = []
    username = None
    
    try:
        st = file_path.stat()
        
        # Get username of file owner
        try:
            username = pwd.getpwuid(st.st_uid).pw_name
        except KeyError:
            username = f"uid:{st.st_uid}"
        
        # Check group ownership (should be m2986)
        try:
            group_name = grp.getgrgid(st.st_gid).gr_name
            if group_name != 'm2986':
                issues.append(f"wrong group '{group_name}' (should be m2986)")
        except KeyError:
            issues.append(f"unknown group ID {st.st_gid} (should be m2986)")
        
        # Check permissions
        mode = st.st_mode
        
        if is_config_file:
            # Configuration files should be read-only (444 or 644)
            if mode & stat.S_IWUSR or mode & stat.S_IWGRP or mode & stat.S_IWOTH:
                issues.append("config file should be read-only")
        else:
            # Other files should have group read/write/execute permissions
            if not (mode & stat.S_IRGRP):
                issues.append("missing group read permission")
            if not (mode & stat.S_IWGRP):
                issues.append("missing group write permission")
            # Only check execute if it's a directory or already executable by owner
            if file_path.is_dir() or (mode & stat.S_IXUSR):
                if not (mode & stat.S_IXGRP):
                    issues.append("missing group execute permission")
                    
    except OSError as e:
        issues.append(f"cannot check permissions: {e}")
    
    return issues, username


def _scan_permissions(ensemble_dir: Path, check_permissions: bool = False) -> Tuple[Dict[str, List[str]], Set[str]]:
    """
    Scan directory tree for permission issues.
    
    Args:
        ensemble_dir: Root ensemble directory to scan
        check_permissions: Whether to actually check permissions
    
    Returns:
        Tuple of (dict mapping file paths to permission issues, set of usernames with issues)
    """
    if not check_permissions:
        return {}, set()
    
    permission_issues = {}
    users_with_issues = set()
    
    # Check cnfg directory and configuration files
    cnfg_dir = ensemble_dir / 'cnfg'
    if cnfg_dir.exists():
        # Check cnfg directory itself
        issues, username = _check_file_permissions(cnfg_dir, is_config_file=False)
        if issues:
            permission_issues[str(cnfg_dir)] = issues
            if username:
                users_with_issues.add(username)
            
        # Check configuration files (lat.* files should be read-only)
        for file_path in cnfg_dir.iterdir():
            if file_path.is_file():
                is_config = 'lat' in file_path.name
                issues, username = _check_file_permissions(file_path, is_config_file=is_config)
                if issues:
                    permission_issues[str(file_path)] = issues
                    if username:
                        users_with_issues.add(username)
    
    # Check measurement directories and their contents
    for item in ensemble_dir.iterdir():
        if item.is_dir() and item.name != 'cnfg':
            # Check measurement directory
            issues, username = _check_file_permissions(item, is_config_file=False)
            if issues:
                permission_issues[str(item)] = issues
                if username:
                    users_with_issues.add(username)
            
            # Recursively check files in measurement directories
            for root, dirs, files in os.walk(str(item)):
                root_path = Path(root)
                # Check subdirectories
                for dir_name in dirs:
                    dir_path = root_path / dir_name
                    issues, username = _check_file_permissions(dir_path, is_config_file=False)
                    if issues:
                        permission_issues[str(dir_path)] = issues
                        if username:
                            users_with_issues.add(username)
                
                # Check files
                for file_name in files:
                    file_path = root_path / file_name
                    issues, username = _check_file_permissions(file_path, is_config_file=False)
                    if issues:
                        permission_issues[str(file_path)] = issues
                        if username:
                            users_with_issues.add(username)
    
    return permission_issues, users_with_issues


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


def _dir_latest_mtime_and_count(root: Path, file_pred=None) -> Tuple[int, float]:
    """
    Return (count, latest_mtime) for files under root matching file_pred.
    file_pred: callable(Path) -> bool; if None, count all regular files.
    """
    count = 0
    latest = 0.0
    if not root.exists() or not root.is_dir():
        return 0, 0.0
    for p in root.iterdir():
        if not p.is_file():
            continue
        if file_pred and not file_pred(p):
            continue
        try:
            st = p.stat()
        except OSError:
            continue
        count += 1
        if st.st_mtime > latest:
            latest = st.st_mtime
    return count, latest


def _scan_measurements(ensemble_dir: Path) -> Dict[str, object]:
    """
    Scan for measurement outputs and return a dict mapping metric keys to
    (count, latest_mtime). Keys: smear_count, t0_count, meson2pt_count, mres_count, zv_count
    """
    results: Dict[str, object] = {}

    # Smearing: sum across cnfg_* directories, count files that look like outputs
    smear_total = 0
    smear_mtime = 0.0
    smear_types: Dict[str, Tuple[int, float]] = {}
    smear_types_meta: Dict[str, Dict[str, Optional[int]]] = {}
    smear_cfgs_union: set = set()
    # helper to extract numeric config from filename (last number)
    def _extract_last_int(name: str) -> Optional[int]:
        m = list(re.finditer(r"(\d+)", name))
        if not m:
            return None
        try:
            return int(m[-1].group(1))
        except Exception:
            return None
    for d in ensemble_dir.iterdir() if ensemble_dir.exists() else []:
        if d.is_dir() and d.name.startswith('cnfg_'):
            # Count files with a trailing number in the name (common for smeared cfgs)
            def smear_pred(p: Path) -> bool:
                return any(ch.isdigit() for ch in p.name) and not p.name.endswith('.in')
            # Build unique config set per type
            uniq: set = set()
            latest = 0.0
            if d.exists():
                for p in d.iterdir():
                    if not p.is_file() or not smear_pred(p):
                        continue
                    cfg = _extract_last_int(p.name)
                    if cfg is not None:
                        uniq.add(cfg)
                    try:
                        st = p.stat()
                        if st.st_mtime > latest:
                            latest = st.st_mtime
                    except OSError:
                        pass
            c = len(uniq)
            m = latest
            smear_total += c
            smear_mtime = max(smear_mtime, m)
            smear_type = d.name[len('cnfg_'):]
            prev_c, prev_m = smear_types.get(smear_type, (0, 0.0))
            smear_types[smear_type] = (prev_c + c, max(prev_m, m))
            # meta per smear type
            if uniq:
                vals = sorted(uniq)
                inc = _infer_increment(vals)
                smear_types_meta[smear_type] = {
                    'first': vals[0],
                    'last': vals[-1],
                    'increment': inc,
                    'total': len(vals)
                }
                smear_cfgs_union.update(uniq)
    results['smear'] = (smear_total, smear_mtime)
    results['smear_types'] = smear_types
    # overall smear meta from union of types
    if smear_cfgs_union:
        vals = sorted(smear_cfgs_union)
        results['smear_meta'] = {
            'first': vals[0], 'last': vals[-1], 'increment': _infer_increment(vals), 'total': len(vals)
        }
    results['smear_types_meta'] = smear_types_meta

    # t0 (wflow): files in t0/ matching t0.*.out by default
    t0_dir = ensemble_dir / 't0'
    def t0_pred(p: Path) -> bool:
        return p.name.startswith('t0.') and p.name.endswith('.out')
    # dedupe by config number
    t0_cfgs: set = set()
    t0_latest = 0.0
    if t0_dir.exists():
        for p in t0_dir.iterdir():
            if not p.is_file() or not t0_pred(p):
                continue
            m = re.match(r"^t0\.(\d+)\.out$", p.name)
            if m:
                try:
                    t0_cfgs.add(int(m.group(1)))
                except Exception:
                    pass
            try:
                st = p.stat()
                if st.st_mtime > t0_latest:
                    t0_latest = st.st_mtime
            except OSError:
                pass
    results['t0'] = (len(t0_cfgs), t0_latest)
    if t0_cfgs:
        vals = sorted(t0_cfgs)
        results['t0_meta'] = {
            'first': vals[0], 'last': vals[-1], 'increment': _infer_increment(vals), 'total': len(vals)
        }

    # meson2pt: prefer meson2pt/DATA directories
    meson_total = 0
    meson_mtime = 0.0
    primary_m2 = ensemble_dir / 'meson2pt' / 'DATA'
    # collect unique cfg numbers from 'ckn<cfg>.bin'
    m2_cfgs: set = set()
    def m2_scan_dir(d: Path):
        nonlocal meson_mtime
        if not d.exists():
            return
        for p in d.iterdir():
            if not p.is_file():
                continue
            n = p.name
            if not (n.startswith('Meson_2pt_') and n.endswith('.bin')):
                continue
            m = re.search(r"ckn(\d+)\.bin$", n)
            if m:
                try:
                    m2_cfgs.add(int(m.group(1)))
                except Exception:
                    pass
            try:
                st = p.stat()
                if st.st_mtime > meson_mtime:
                    meson_mtime = st.st_mtime
            except OSError:
                pass
    m2_scan_dir(primary_m2)
    # Also scan any meson2pt* work directories that contain DATA
    for d in ensemble_dir.iterdir() if ensemble_dir.exists() else []:
        if d.is_dir() and d.name.lower().startswith('meson2pt') and (d / 'DATA').exists():
            m2_scan_dir(d / 'DATA')
    if m2_cfgs:
        vals = sorted(m2_cfgs)
        results['meson2pt_meta'] = {
            'first': vals[0], 'last': vals[-1], 'increment': _infer_increment(vals), 'total': len(vals)
        }
    results['meson2pt'] = (len(m2_cfgs), meson_mtime)

    # mres: union unique cfgs across all mres*/DATA
    mres_cfgs: set = set()
    mres_total = 0
    mres_mtime = 0.0
    for d in ensemble_dir.iterdir() if ensemble_dir.exists() else []:
        if d.is_dir() and (d.name == 'mres' or d.name.startswith('mres_')):
            if (d / 'DATA').exists():
                # dedupe by trailing number in filename
                uniq: set = set()
                latest = 0.0
                for p in (d / 'DATA').iterdir():
                    if not p.is_file():
                        continue
                    # filenames like Mres_0ckn12.bin -> capture 12
                    m = re.search(r"ckn(\d+)\.bin$", p.name)
                    cfg = int(m.group(1)) if m else _extract_last_int(p.name)
                    if cfg is not None:
                        uniq.add(cfg)
                    try:
                        st = p.stat()
                        if st.st_mtime > latest:
                            latest = st.st_mtime
                    except OSError:
                        pass
                c, m = len(uniq), latest
            else:
                c, m = _dir_latest_mtime_and_count(d, lambda p: not p.name.endswith('.in') and p.name != 'jlog')
            mres_cfgs.update(uniq if 'uniq' in locals() else [])
            mres_total = len(mres_cfgs) if mres_cfgs else (mres_total + c)
            mres_mtime = max(mres_mtime, m)
    results['mres'] = (len(mres_cfgs) if mres_cfgs else mres_total, mres_mtime)
    if mres_cfgs:
        vals = sorted(mres_cfgs)
        results['mres_meta'] = {
            'first': vals[0], 'last': vals[-1], 'increment': _infer_increment(vals), 'total': len(vals)
        }

    # Zv: look under Zv/ or Zv_* directories; prefer DATA/*.bin
    zv_cfgs: set = set()
    zv_total = 0
    zv_mtime = 0.0
    for d in ensemble_dir.iterdir() if ensemble_dir.exists() else []:
        if d.is_dir() and (d.name == 'Zv' or d.name.startswith('Zv')):
            if (d / 'DATA').exists():
                uniq: set = set()
                latest = 0.0
                for p in (d / 'DATA').iterdir():
                    if not p.is_file() or p.suffix != '.bin':
                        continue
                    # filenames like FDiagonal_2pt_ckn448.bin -> capture 448
                    m = re.search(r"ckn(\d+)\.bin$", p.name)
                    cfg = int(m.group(1)) if m else _extract_last_int(p.name)
                    if cfg is not None:
                        uniq.add(cfg)
                    try:
                        st = p.stat()
                        if st.st_mtime > latest:
                            latest = st.st_mtime
                    except OSError:
                        pass
                c, m = len(uniq), latest
            else:
                c, m = _dir_latest_mtime_and_count(d, lambda p: p.suffix == '.bin')
            zv_cfgs.update(uniq if 'uniq' in locals() else [])
            zv_total = len(zv_cfgs) if zv_cfgs else (zv_total + c)
            zv_mtime = max(zv_mtime, m)
    results['zv'] = (len(zv_cfgs) if zv_cfgs else zv_total, zv_mtime)
    if zv_cfgs:
        vals = sorted(zv_cfgs)
        results['zv_meta'] = {
            'first': vals[0], 'last': vals[-1], 'increment': _infer_increment(vals), 'total': len(vals)
        }

    return results


def register(subparsers):
    p = subparsers.add_parser(
        'scan',
        help='Scan ensemble directories for configurations and measurements',
        description="""
Scan ensemble directories to detect gauge configurations and measurement files.

WHAT THIS DOES:
• Scans cnfg/ directories to detect configuration ranges (first, last, increment, total)
• Scans measurement directories (smearing, t0, meson2pt, mres, Zv) to count completed files
• Stores all detected ranges and counts in ensemble parameters for tracking
• Optionally checks file permissions and ownership
• Optionally scans filesystem for ensembles not yet in the database

INCREMENTAL SCANNING:
By default, only rescans ensembles where the cnfg/ directory has changed
(different file count or modification time). Use --force to rescan all ensembles.

PERMISSION CHECKING:
With --check-permissions, verifies that:
• Configuration files (lat.*) are read-only
• All other files have group read/write/execute permissions
• All files are owned by the m2986 group

FILESYSTEM SCANNING:
With --scan-fs, also searches the filesystem under TUNING/ and ENSEMBLES/
for ensemble directories not yet registered in the database.

EXAMPLES:
  # Scan all ensembles (only changed ones)
  mdwf_db scan

  # Force rescan of all ensembles
  mdwf_db scan --force

  # Scan with permission checking
  mdwf_db scan --check-permissions

  # Scan and report unregistered ensembles
  mdwf_db scan --scan-fs

  # Full scan with all options
  mdwf_db scan --force --check-permissions --scan-fs --base-dir /path/to/data
        """
    )
    p.add_argument(
        '--scan-fs',
        action='store_true',
        help='Also scan filesystem under base dir for ensembles not in DB and report their ranges'
    )
    p.add_argument(
        '--force',
        action='store_true',
        help='Force rescan and refresh stats even if nothing appears to have changed'
    )
    p.add_argument(
        '--check-permissions',
        action='store_true',
        help='Check file permissions and group ownership (config files should be read-only, others should have group rwX, all should be owned by m2986 group)'
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
    total_permission_issues = 0
    
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

        if not getattr(args, 'force', False):
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

            # Scan measurement outputs and persist summary stats
            meas = _scan_measurements(ens_dir)
            set_ensemble_parameter(args.db_file, ens['id'], 'smear_count', str(meas.get('smear', (0,0.0))[0]))
            set_ensemble_parameter(args.db_file, ens['id'], 'smear_mtime', str(meas.get('smear', (0,0.0))[1]))
            # Persist per-smear-type counts/ranges and mtimes under keys like smear_STOUT8_*
            for stype, (scount, smt) in (meas.get('smear_types') or {}).items():
                key_base = f"smear_{stype}"
                set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_count", str(scount))
                set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_mtime", str(smt))
            for stype, meta in (meas.get('smear_types_meta') or {}).items():
                key_base = f"smear_{stype}"
                set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_first", str(meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_last", str(meta.get('last')))
                inc = meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_increment", str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], f"{key_base}_total", str(meta.get('total')))
            # Persist overall smear range if available
            smear_meta = meas.get('smear_meta') or {}
            if smear_meta:
                set_ensemble_parameter(args.db_file, ens['id'], 'smear_first', str(smear_meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], 'smear_last', str(smear_meta.get('last')))
                inc = smear_meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], 'smear_increment', str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], 'smear_total', str(smear_meta.get('total')))
            set_ensemble_parameter(args.db_file, ens['id'], 't0_count', str(meas.get('t0', (0,0.0))[0]))
            set_ensemble_parameter(args.db_file, ens['id'], 't0_mtime', str(meas.get('t0', (0,0.0))[1]))
            t0_meta = meas.get('t0_meta') or {}
            if t0_meta:
                set_ensemble_parameter(args.db_file, ens['id'], 't0_first', str(t0_meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], 't0_last', str(t0_meta.get('last')))
                inc = t0_meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], 't0_increment', str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], 't0_total', str(t0_meta.get('total')))
            set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_count', str(meas.get('meson2pt', (0,0.0))[0]))
            set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_mtime', str(meas.get('meson2pt', (0,0.0))[1]))
            m2_meta = meas.get('meson2pt_meta') or {}
            if m2_meta:
                set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_first', str(m2_meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_last', str(m2_meta.get('last')))
                inc = m2_meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_increment', str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], 'meson2pt_total', str(m2_meta.get('total')))
            set_ensemble_parameter(args.db_file, ens['id'], 'mres_count', str(meas.get('mres', (0,0.0))[0]))
            set_ensemble_parameter(args.db_file, ens['id'], 'mres_mtime', str(meas.get('mres', (0,0.0))[1]))
            mres_meta = meas.get('mres_meta') or {}
            if mres_meta:
                set_ensemble_parameter(args.db_file, ens['id'], 'mres_first', str(mres_meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], 'mres_last', str(mres_meta.get('last')))
                inc = mres_meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], 'mres_increment', str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], 'mres_total', str(mres_meta.get('total')))
            set_ensemble_parameter(args.db_file, ens['id'], 'zv_count', str(meas.get('zv', (0,0.0))[0]))
            set_ensemble_parameter(args.db_file, ens['id'], 'zv_mtime', str(meas.get('zv', (0,0.0))[1]))
            zv_meta = meas.get('zv_meta') or {}
            if zv_meta:
                set_ensemble_parameter(args.db_file, ens['id'], 'zv_first', str(zv_meta.get('first')))
                set_ensemble_parameter(args.db_file, ens['id'], 'zv_last', str(zv_meta.get('last')))
                inc = zv_meta.get('increment'); set_ensemble_parameter(args.db_file, ens['id'], 'zv_increment', str(inc) if inc is not None else '')
                set_ensemble_parameter(args.db_file, ens['id'], 'zv_total', str(zv_meta.get('total')))

            updated += 1
        except Exception as e:
            print(f"WARNING: Failed to set config/meas ranges for ensemble {ens['id']}: {e}", file=sys.stderr)
        
        # Check permissions if requested
        if getattr(args, 'check_permissions', False):
            permission_issues, users_with_issues = _scan_permissions(ens_dir, check_permissions=True)
            if permission_issues:
                # Include nickname if available
                nickname = ens.get('nickname')
                ens_display = f"ensemble {ens['id']}"
                if nickname:
                    ens_display += f" ({nickname})"
                
                # Format user list
                users_str = ", ".join(sorted(users_with_issues)) if users_with_issues else "unknown"
                
                print(f"Permission issues in {ens_display} at {ens_dir}: {len(permission_issues)} file(s) with wrong permissions (users: {users_str})")
                total_permission_issues += len(permission_issues)

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
    if getattr(args, 'check_permissions', False):
        if total_permission_issues > 0:
            msg += f"; found {total_permission_issues} permission issue(s)"
        else:
            msg += "; all permissions OK"
    print(msg)
    return 0


