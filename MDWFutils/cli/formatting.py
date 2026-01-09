"""Shared formatting utilities for CLI output."""

import math
from typing import Any, Dict, List, Union


def safe_float(val: Any, default: float = 999.0) -> float:
    """Safely convert value to float, returning default on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_int(val: Any, default: int = 999) -> int:
    """Safely convert value to int, returning default on failure."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def format_float(value: Any) -> str:
    """Format a float value, handling NaN and None."""
    if value is None:
        return ''
    try:
        if math.isnan(value):
            return 'NaN'
        return f"{value:.5f}".rstrip('0').rstrip('.')
    except (TypeError, ValueError):
        return str(value)


def format_sci(value: Any) -> str:
    """Format a float in scientific notation for correlator values."""
    if value is None:
        return ''
    try:
        if math.isnan(value):
            return 'NaN'
        # Use scientific notation for very small/large values
        if abs(value) < 1e-3 or abs(value) > 1e5:
            return f"{value:.4e}"
        return f"{value:.6f}".rstrip('0').rstrip('.')
    except (TypeError, ValueError):
        return str(value)


def print_table(headers: List[str], rows: List[Union[Dict, List]]) -> None:
    """Print a formatted table with proper column alignment.
    
    Args:
        headers: List of column header names
        rows: List of rows, either as dicts (keyed by header) or lists
    """
    if not rows:
        return
    
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
