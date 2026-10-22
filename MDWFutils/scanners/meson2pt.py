"""Scanner for meson 2pt measurement files."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from .base import BaseScanner, ScanResult


_NUM = r'(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?'
_TOKEN_RE = re.compile(rf'^(ml|ms|mc)({_NUM})$')
_FOLDER_RE = re.compile(rf'^meson2pt(?:_(?:ml|ms|mc){_NUM})*$')


def parse_variant_label(name: str) -> Optional[Dict[str, float]]:
    """Parse a meson2pt folder label into its mass overrides.

    Returns None for names that are not valid variant folder names,
    {} for the plain 'meson2pt' folder, and {key: float} otherwise.
    """
    if not _FOLDER_RE.match(name):
        return None
    parts = name.split('_')
    overrides: Dict[str, float] = {}
    for token in parts[1:]:
        match = _TOKEN_RE.match(token)
        if not match or match.group(1) in overrides:
            return None
        overrides[match.group(1)] = float(match.group(2))
    return overrides


def make_measurement_type(overrides: Dict[str, float]) -> str:
    """Build the measurement_type string for a set of mass overrides."""
    if not overrides:
        return "meson2pt"
    return "meson2pt_" + "_".join(
        f"{k}{overrides[k]}" for k in ("ml", "ms", "mc")
        if overrides.get(k) is not None
    )


@dataclass
class Meson2ptVariant:
    """A discovered meson2pt measurement folder."""
    name: str
    dir: Path
    overrides: Dict[str, float] = field(default_factory=dict)


def discover_variants(ensemble_path: Path) -> List[Meson2ptVariant]:
    """Find meson2pt and meson2pt_* variant folders with a DATA/ subdir."""
    variants: List[Meson2ptVariant] = []
    try:
        entries = list(ensemble_path.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_dir():
            continue
        if entry.name != 'meson2pt' and not _FOLDER_RE.match(entry.name):
            continue
        if not (entry / 'DATA').is_dir():
            continue
        overrides = parse_variant_label(entry.name)
        if overrides is None:
            continue
        variants.append(Meson2ptVariant(name=entry.name, dir=entry, overrides=overrides))
    variants.sort(key=lambda v: (v.name != 'meson2pt', v.name))
    return variants


class Meson2ptScanner(BaseScanner):
    """Scanner for unitary meson 2pt measurement files.
    
    Finds files matching: meson2pt/DATA/Meson_2pt_{qq}ckn{cfg}.bin
    where {qq} is the quark pair code.
    
    Returns one ScanResult per config that has at least one meson file.
    """
    
    # Meson name to quark pair code mapping
    MESONS = {
        'pion': '00',    # light-light
        'kaon': '01',    # light-strange
        'eta_s': '11',   # strange-strange
        'D': '02',       # light-charm
        'Ds': '12',      # strange-charm
        'eta_c': '22',   # charm-charm
    }
    
    # Reverse mapping: code to name
    CODE_TO_MESON = {v: k for k, v in MESONS.items()}

    def __init__(self, variant_dir: Optional[Path] = None):
        """Initialize scanner.

        Args:
            variant_dir: Variant folder (e.g. meson2pt_ml0.0004_mc0.1234) whose
                DATA/ subdirectory to scan; defaults to 'meson2pt'.
        """
        self.variant_dir = variant_dir

    
    def scan(self, ensemble_path: Path) -> List[ScanResult]:
        """Find all meson2pt files and group by config.
        
        Args:
            ensemble_path: Path to ensemble root directory
            
        Returns:
            List of ScanResult objects, one per config with at least one meson file
        """
        meson_dir = (self.variant_dir or ensemble_path / 'meson2pt') / 'DATA'
        if not meson_dir.exists():
            return []
        
        # Pattern: Meson_2pt_{qq}ckn{cfg}.bin
        pattern = re.compile(r'^Meson_2pt_(\d{2})ckn(\d+)\.bin$')
        
        # Group files by config number: {cfg: {meson_name: path}}
        files_by_config: Dict[int, Dict[str, Path]] = defaultdict(dict)
        
        for file_path in meson_dir.iterdir():
            if not file_path.is_file():
                continue
            
            match = pattern.match(file_path.name)
            if match:
                quark_code = match.group(1)
                cfg_num = int(match.group(2))
                
                # Convert quark code to meson name
                meson_name = self.CODE_TO_MESON.get(quark_code)
                if meson_name:
                    files_by_config[cfg_num][meson_name] = file_path
        
        # Return configs that have at least one meson file
        results = []
        for cfg_num, meson_files in files_by_config.items():
            if meson_files:
                # Store all file paths in metadata
                metadata = {
                    'files': {name: str(path) for name, path in meson_files.items()},
                    'mesons_found': list(meson_files.keys()),
                }
                
                # Use first meson file as primary file_path
                first_meson = next(iter(meson_files.values()))
                results.append(ScanResult(
                    file_path=first_meson,
                    config_number=cfg_num,
                    metadata=metadata,
                ))
        
        return sorted(results, key=lambda r: r.config_number)
