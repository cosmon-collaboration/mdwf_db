# Store Actual Config List for Missing Detection

## Overview

Store the actual list of configuration numbers in `ensemble.configurations.config_list`, extracted from `ckpoint_EODWF_lat.{number}` files in the `cnfg/` directory. Use this list for accurate missing measurement detection.

## Current State

```python
configurations: {
    first: 1,
    last: 728,
    increment: None,  # Often None for non-uniform spacing
    total: 159,
}
```

Problem: Can't compute expected configs without `increment`.

## New State

```python
configurations: {
    first: 1,
    last: 728,
    increment: None,
    total: 159,
    config_list: [1, 4, 8, 12, 16, ...]  # NEW: actual config numbers
}
```



## Implementation

### 1. Update `_extract_cfg_numbers()` in `scan_configs.py`

Change from generic digit extraction to specific pattern matching:

```python
def _extract_cfg_numbers(cnfg_dir: Path):
    """Extract config numbers from ckpoint_EODWF_lat.{number} files."""
    if not cnfg_dir.exists():
        return []
    pattern = re.compile(r'^ckpoint_EODWF_lat\.(\d+)$')
    numbers = []
    for child in cnfg_dir.iterdir():
        if not child.is_file():
            continue
        m = pattern.match(child.name)
        if m:
            numbers.append(int(m.group(1)))
    return sorted(set(numbers))
```



### 2. Update `do_scan()` to store `config_list`

```python
backend.update_ensemble(
    ens_id,
    configurations={
        'first': first,
        'last': last,
        'increment': increment,
        'total': total,
        'config_list': values,  # NEW: store the actual list
    },
)
```



### 3. Update schema documentation in `ensemble.py`

```python
"configurations": {
    "first": int | None,
    "last": int | None,
    "increment": int | None,
    "total": int | None,
    "config_list": list[int],  # NEW
},
```



### 4. Update `--missing` handler in `query.py`

Use `config_list` instead of computing from first/last/increment:

```python
if args.missing:
    cfg = ensemble.get('configurations', {})
    config_list = cfg.get('config_list', [])
    if not config_list:
        print(f"ERROR: No config list for ensemble {ensemble_id} (run 'mdwf scan')")
        return 1
    
    expected = set(config_list)
    measured = set(backend.get_measured_configs(ensemble_id, args.missing))
    missing = sorted(expected - measured)
    ...
```



### 5. Update `_report_missing()` in `scan_configs.py`

Same logic - use `config_list` from the ensemble.

## Files Modified

| File | Changes |

|------|---------|

| `scan_configs.py` | Update `_extract_cfg_numbers()` pattern, store `config_list` |

| `query.py` | Use `config_list` for `--missing` |

| `ensemble.py` | Document `config_list` field |

## Migration