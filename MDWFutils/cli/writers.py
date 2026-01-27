"""Output writers for exporting measurement data."""

import json
import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np

try:
    import h5py
    HAS_H5PY = True
except ImportError:
    HAS_H5PY = False


# Field definitions for each measurement type
GAUGE_OBS_FIELDS = ['plaq', 'Q', 'sqrt_t0_clov', 'sqrt_t0_plaq', 'w0_clov', 'w0_plaq']

MRES_QUARKS = ['light', 'strange', 'charm']
MRES_CORRELATORS = ['PP', 'MP']

MESON_ORDER = ['pion', 'kaon', 'eta_s', 'D', 'Ds', 'eta_c']
MESON_CORRELATORS = ['PP', 'AP']


def _get_int_dtype(values):
    """Determine the smallest appropriate integer dtype for a list of values.
    
    Returns the smallest dtype that can hold all values:
    - uint8: 0-255
    - uint16: 0-65535
    - uint32: 0-4294967295
    - int8: -128 to 127
    - int16: -32768 to 32767
    - int32: -2147483648 to 2147483647
    """
    if not values:
        return np.int16
    
    # Filter out None values and convert to list if needed
    try:
        clean_values = [v for v in values if v is not None]
        if not clean_values:
            return np.int16
        
        max_val = max(clean_values)
        min_val = min(clean_values)
    except (TypeError, ValueError):
        # If values aren't comparable (e.g., mixed types), default to int16
        return np.int16
    
    if min_val >= 0:
        if max_val <= 255:
            return np.uint8
        elif max_val <= 65535:
            return np.uint16
        else:
            return np.uint32
    else:
        if min_val >= -128 and max_val <= 127:
            return np.int8
        elif min_val >= -32768 and max_val <= 32767:
            return np.int16
        else:
            return np.int32


def get_ensemble_name(ensemble: Dict[str, Any]) -> str:
    """Derive HDF5 path key from ensemble.
    
    Uses nickname if available, otherwise derives from directory path.
    Follows legacy format: parts after ENSEMBLES or TUNING joined with underscore.
    """
    if ensemble.get('nickname'):
        return ensemble['nickname']
    
    directory = ensemble.get('directory', '')
    path = Path(directory)
    parts = list(path.parts)
    
    # Find ENSEMBLES or TUNING marker
    for marker in ['ENSEMBLES', 'TUNING']:
        if marker in parts:
            idx = parts.index(marker)
            # Take everything after the marker
            relevant = parts[idx + 1:]
            if relevant:
                return '_'.join(relevant)
    
    # Fallback: use last few path components
    if len(parts) >= 3:
        return '_'.join(parts[-3:])
    return '_'.join(parts) if parts else f"ensemble_{ensemble.get('ensemble_id', 'unknown')}"


def expand_fields(fields: Optional[List[str]], measurement_type: str) -> Set[str]:
    """Expand field shorthand to full field names.
    
    For mres: 'light' expands to {'light_PP', 'light_MP'}
    For meson2pt: 'pion' expands to {'pion_PP', 'pion_AP'}
    
    Args:
        fields: List of field names or None for all fields
        measurement_type: One of 'gauge_obs', 'mres', 'meson2pt'
        
    Returns:
        Set of expanded field names
    """
    if measurement_type == 'gauge_obs':
        if fields is None:
            return set(GAUGE_OBS_FIELDS)
        # gauge_obs fields are already leaf-level
        return set(f for f in fields if f in GAUGE_OBS_FIELDS)
    
    elif measurement_type == 'mres':
        if fields is None:
            # All quarks, all correlators
            return {f"{q}_{c}" for q in MRES_QUARKS for c in MRES_CORRELATORS}
        
        expanded = set()
        for f in fields:
            if f in MRES_QUARKS:
                # Expand quark to both correlators
                expanded.update(f"{f}_{c}" for c in MRES_CORRELATORS)
            elif '_' in f:
                # Already fully specified (e.g., 'light_PP')
                expanded.add(f)
        return expanded
    
    elif measurement_type == 'meson2pt':
        if fields is None:
            # All mesons, all correlators
            return {f"{m}_{c}" for m in MESON_ORDER for c in MESON_CORRELATORS}
        
        expanded = set()
        for f in fields:
            if f in MESON_ORDER:
                # Expand meson to both correlators
                expanded.update(f"{f}_{c}" for c in MESON_CORRELATORS)
            elif '_' in f:
                # Already fully specified (e.g., 'pion_PP')
                expanded.add(f)
        return expanded
    
    return set()


def write_data(
    data: Dict[str, Any],
    output_path: Path,
    measurement_type: str,
) -> None:
    """Write data to file based on extension.
    
    Args:
        data: Dictionary with ensemble_name keys containing measurement data
        output_path: Path to output file
        measurement_type: One of 'gauge_obs', 'mres', 'meson2pt', 'all'
    """
    ext = output_path.suffix.lower()
    
    if ext in ('.h5', '.hdf5'):
        write_hdf5(data, output_path, measurement_type)
    elif ext == '.csv':
        write_csv(data, output_path, measurement_type)
    elif ext == '.json':
        write_json(data, output_path)
    else:
        raise ValueError(f"Unsupported file extension: {ext}. Use .h5, .csv, or .json")


def write_hdf5(
    data: Dict[str, Any],
    output_path: Path,
    measurement_type: str,
) -> None:
    """Write data to HDF5 in legacy-compatible format.
    
    Format:
        gauge_obs:  /{ensemble}/cfgs, plaq, Q, ...
        meson2pt:   /{ensemble}/unitary/cfgs, pion_PP, pion_AP, ...
        mres:       /{ensemble}/unitary/mq{mass}/cfgs, PP, MP
    """
    if not HAS_H5PY:
        raise ImportError("h5py is required for HDF5 output. Install with: pip install h5py")
    
    with h5py.File(output_path, 'a') as f5:
        for ensemble_name, ensemble_data in data.items():
            _write_ensemble_hdf5(f5, ensemble_name, ensemble_data, measurement_type)


def _write_ensemble_hdf5(
    f5: 'h5py.File',
    ensemble_name: str,
    ensemble_data: Dict[str, Any],
    measurement_type: str,
) -> None:
    """Write a single ensemble's data to HDF5."""
    
    if measurement_type in ('gauge_obs', 'all'):
        gauge_data = ensemble_data.get('gauge_obs', {})
        if gauge_data:
            _write_gauge_obs_hdf5(f5, ensemble_name, gauge_data)
    
    if measurement_type in ('meson2pt', 'all'):
        meson_data = ensemble_data.get('meson2pt', {})
        if meson_data:
            _write_meson2pt_hdf5(f5, ensemble_name, meson_data)
    
    if measurement_type in ('mres', 'all'):
        mres_data = ensemble_data.get('mres', {})
        if mres_data:
            _write_mres_hdf5(f5, ensemble_name, mres_data)


def _write_gauge_obs_hdf5(f5: 'h5py.File', ensemble_name: str, data: Dict[str, Any]) -> None:
    """Write gauge observables to HDF5.
    
    Format: /{ensemble}/cfgs, plaq, Q, sqrt_t0_clov, sqrt_t0_plaq, w0_clov, w0_plaq
    """
    cfgs = data.get('cfgs', [])
    
    # Delete existing datasets if present
    for key in ['cfgs'] + GAUGE_OBS_FIELDS:
        path = f"{ensemble_name}/{key}"
        if path in f5:
            del f5[path]
    
    # Write config numbers with appropriate dtype
    cfg_dtype = _get_int_dtype(cfgs)
    f5.create_dataset(f"{ensemble_name}/cfgs", data=np.array(cfgs, dtype=cfg_dtype))
    
    # Write each field
    for field in GAUGE_OBS_FIELDS:
        if field in data:
            f5.create_dataset(f"{ensemble_name}/{field}", data=np.array(data[field]))


def _write_meson2pt_hdf5(f5: 'h5py.File', ensemble_name: str, data: Dict[str, Any]) -> None:
    """Write meson 2pt correlators to HDF5.
    
    Format: /{ensemble}/unitary/cfgs, pion_PP, pion_AP, kaon_PP, ...
    Also writes ensemble_config_list if available at /{ensemble}/unitary/config_list
    """
    base_path = f"{ensemble_name}/unitary"
    cfgs = data.get('cfgs', [])
    
    # Delete existing cfgs if present
    if f"{base_path}/cfgs" in f5:
        del f5[f"{base_path}/cfgs"]
    
    # Write config numbers with appropriate dtype
    cfg_dtype = _get_int_dtype(cfgs)
    f5.create_dataset(f"{base_path}/cfgs", data=np.array(cfgs, dtype=cfg_dtype))
    
    # Write ensemble config_list if available
    if 'ensemble_config_list' in data:
        config_list = data['ensemble_config_list']
        list_path = f"{base_path}/config_list"
        if list_path in f5:
            del f5[list_path]
        list_dtype = _get_int_dtype(config_list)
        f5.create_dataset(list_path, data=np.array(config_list, dtype=list_dtype))
    
    # Write each meson correlator
    for meson in MESON_ORDER:
        for corr in MESON_CORRELATORS:
            key = f"{meson}_{corr}"
            if key in data:
                path = f"{base_path}/{key}"
                if path in f5:
                    del f5[path]
                f5.create_dataset(path, data=np.array(data[key]))


def _write_mres_hdf5(f5: 'h5py.File', ensemble_name: str, data: Dict[str, Any]) -> None:
    """Write mres correlators to HDF5.
    
    Format: /{ensemble}/unitary/mq{mass}/cfgs, PP, MP
    """
    base_path = f"{ensemble_name}/unitary"
    
    # Data is organized by quark mass
    for quark_key, quark_data in data.items():
        if not quark_key.startswith('mq'):
            continue
        
        quark_path = f"{base_path}/{quark_key}"
        cfgs = quark_data.get('cfgs', [])
        
        # Delete and recreate
        if f"{quark_path}/cfgs" in f5:
            del f5[f"{quark_path}/cfgs"]
        cfg_dtype = _get_int_dtype(cfgs)
        f5.create_dataset(f"{quark_path}/cfgs", data=np.array(cfgs, dtype=cfg_dtype))
        
        for corr in MRES_CORRELATORS:
            if corr in quark_data:
                path = f"{quark_path}/{corr}"
                if path in f5:
                    del f5[path]
                f5.create_dataset(path, data=np.array(quark_data[corr]))


def write_csv(
    data: Dict[str, Any],
    output_path: Path,
    measurement_type: str,
) -> None:
    """Write data to CSV format.
    
    For gauge_obs: flat table with ensemble, cfg, and observable columns
    For correlators: one row per (ensemble, cfg, timeslice) with correlator values
    """
    rows = []
    
    for ensemble_name, ensemble_data in data.items():
        if measurement_type in ('gauge_obs', 'all'):
            gauge_data = ensemble_data.get('gauge_obs', {})
            if gauge_data:
                cfgs = gauge_data.get('cfgs', [])
                for i, cfg in enumerate(cfgs):
                    row = {'ensemble': ensemble_name, 'cfg': cfg}
                    for field in GAUGE_OBS_FIELDS:
                        if field in gauge_data:
                            row[field] = gauge_data[field][i] if i < len(gauge_data[field]) else ''
                    rows.append(row)
        
        if measurement_type in ('meson2pt', 'all'):
            meson_data = ensemble_data.get('meson2pt', {})
            if meson_data:
                cfgs = meson_data.get('cfgs', [])
                # Determine T extent (skip cfgs and ensemble_config_list)
                t_extent = 0
                for key in meson_data:
                    if key not in ('cfgs', 'ensemble_config_list') and isinstance(meson_data[key], (list, np.ndarray)):
                        if len(meson_data[key]) > 0:
                            # Check if it's a 2D array (list of lists)
                            if isinstance(meson_data[key][0], (list, np.ndarray)):
                                t_extent = len(meson_data[key][0])
                                break
                
                for i, cfg in enumerate(cfgs):
                    for t in range(t_extent):
                        row = {'ensemble': ensemble_name, 'cfg': cfg, 't': t}
                        for meson in MESON_ORDER:
                            for corr in MESON_CORRELATORS:
                                key = f"{meson}_{corr}"
                                if key in meson_data and i < len(meson_data[key]):
                                    row[key] = meson_data[key][i][t] if t < len(meson_data[key][i]) else ''
                        rows.append(row)
        
        if measurement_type in ('mres', 'all'):
            mres_data = ensemble_data.get('mres', {})
            for quark_key, quark_data in mres_data.items():
                if not quark_key.startswith('mq'):
                    continue
                cfgs = quark_data.get('cfgs', [])
                t_extent = len(quark_data.get('PP', [[]])[0]) if quark_data.get('PP') else 0
                
                for i, cfg in enumerate(cfgs):
                    for t in range(t_extent):
                        row = {
                            'ensemble': ensemble_name,
                            'cfg': cfg,
                            'quark': quark_key,
                            't': t,
                        }
                        for corr in MRES_CORRELATORS:
                            if corr in quark_data and i < len(quark_data[corr]):
                                row[corr] = quark_data[corr][i][t] if t < len(quark_data[corr][i]) else ''
                        rows.append(row)
    
    if not rows:
        return
    
    # Write CSV
    fieldnames = list(rows[0].keys())
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(data: Dict[str, Any], output_path: Path) -> None:
    """Write data to JSON format."""
    
    # Convert numpy arrays to lists for JSON serialization
    def convert(obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(v) for v in obj]
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        return obj
    
    with open(output_path, 'w') as f:
        json.dump(convert(data), f, indent=2)


def prepare_gauge_obs_data(
    measurements: List[Dict[str, Any]],
    fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Prepare gauge_obs measurements for export.
    
    Args:
        measurements: List of measurement documents from database
        fields: Set of field names to include, or None for all
        
    Returns:
        Dict with cfgs and field arrays
    """
    if fields is None:
        fields = set(GAUGE_OBS_FIELDS)
    
    # Sort by config number
    measurements = sorted(measurements, key=lambda m: m.get('config_number', 0))
    
    result = {'cfgs': []}
    for field in fields:
        if field in GAUGE_OBS_FIELDS:
            result[field] = []
    
    for m in measurements:
        result['cfgs'].append(m.get('config_number', 0))
        data = m.get('data', {})
        for field in fields:
            if field in GAUGE_OBS_FIELDS:
                result[field].append(data.get(field, np.nan))
    
    return result


def prepare_meson2pt_data(
    measurements: List[Dict[str, Any]],
    physics: Dict[str, Any],
    fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Prepare meson2pt measurements for export.
    
    Args:
        measurements: List of measurement documents
        physics: Physics parameters (for mass values)
        fields: Set of field names to include (e.g., {'pion_PP', 'kaon_AP'})
        
    Returns:
        Dict with cfgs and correlator arrays
    """
    if fields is None:
        fields = {f"{m}_{c}" for m in MESON_ORDER for c in MESON_CORRELATORS}
    
    measurements = sorted(measurements, key=lambda m: m.get('config_number', 0))
    
    result = {'cfgs': []}
    for field in fields:
        result[field] = []
    
    for m in measurements:
        result['cfgs'].append(m.get('config_number', 0))
        data = m.get('data', {})
        mesons = data.get('mesons', {})
        
        for field in fields:
            parts = field.rsplit('_', 1)
            if len(parts) == 2:
                meson, corr = parts
                if meson in mesons and corr in mesons[meson]:
                    result[field].append(mesons[meson][corr])
                else:
                    result[field].append([])
    
    return result


def prepare_mres_data(
    measurements: List[Dict[str, Any]],
    physics: Dict[str, Any],
    fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Prepare mres measurements for export.
    
    Args:
        measurements: List of measurement documents
        physics: Physics parameters (for mass values)
        fields: Set of field names to include (e.g., {'light_PP', 'strange_MP'})
        
    Returns:
        Dict with mq{mass} keys containing cfgs, PP, MP arrays
    """
    if fields is None:
        fields = {f"{q}_{c}" for q in MRES_QUARKS for c in MRES_CORRELATORS}
    
    measurements = sorted(measurements, key=lambda m: m.get('config_number', 0))
    
    # Map quark names to mass values
    quark_to_mass = {
        'light': physics.get('ml', '0.0'),
        'strange': physics.get('ms', '0.0'),
        'charm': physics.get('mc', '0.0'),
    }
    
    # Determine which quarks we need
    needed_quarks = set()
    for field in fields:
        parts = field.rsplit('_', 1)
        if len(parts) == 2 and parts[0] in MRES_QUARKS:
            needed_quarks.add(parts[0])
    
    result = {}
    for quark in needed_quarks:
        mass = quark_to_mass[quark]
        mq_key = f"mq{mass}"
        result[mq_key] = {'cfgs': [], 'PP': [], 'MP': []}
    
    for m in measurements:
        cfg = m.get('config_number', 0)
        data = m.get('data', {})
        quarks = data.get('quarks', {})
        
        for quark in needed_quarks:
            mass = quark_to_mass[quark]
            mq_key = f"mq{mass}"
            
            if quark in quarks:
                result[mq_key]['cfgs'].append(cfg)
                
                # Add PP if requested
                if f"{quark}_PP" in fields:
                    result[mq_key]['PP'].append(quarks[quark].get('PP', []))
                
                # Add MP if requested
                if f"{quark}_MP" in fields:
                    result[mq_key]['MP'].append(quarks[quark].get('MP', []))
    
    return result
