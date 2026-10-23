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
_SET_PATTERN = r'(?:l|s|c|ls|lc|sc)'
_SET_RE = re.compile(rf'^{_SET_PATTERN}$')
_FOLDER_RE = re.compile(rf'^meson2pt(?:_{_SET_PATTERN})?(?:_(?:ml|ms|mc){_NUM})*$')

QUARK_ORDER = ("l", "s", "c")
QUARK_TO_MASS_KEY = {"l": "ml", "s": "ms", "c": "mc"}


def _parse_variant_tokens(name: str):
    """Return (quarks, overrides) for a folder name, or None if invalid."""
    if not _FOLDER_RE.match(name):
        return None
    quarks = list(QUARK_ORDER)
    overrides: Dict[str, float] = {}
    for token in name.split('_')[1:]:
        match = _TOKEN_RE.match(token)
        if match:
            if match.group(1) in overrides:
                return None
            overrides[match.group(1)] = float(match.group(2))
            continue
        if len(token) <= 3 and _SET_RE.match(token) and quarks == list(QUARK_ORDER) and not overrides:
            quarks = [ch for ch in token]
            continue
        return None
    if any(key not in {QUARK_TO_MASS_KEY[q] for q in quarks} for key in overrides):
        return None
    return quarks, overrides


def parse_variant_label(name: str) -> Optional[Dict[str, float]]:
    """Parse a meson2pt folder label into its mass overrides.

    Returns None for names that are not valid variant folder names,
    {} for the plain 'meson2pt' folder, and {key: float} otherwise.
    """
    parsed = _parse_variant_tokens(name)
    return None if parsed is None else parsed[1]


def parse_variant_quarks(name: str) -> Optional[List[str]]:
    """Parse the run-set from a meson2pt folder label; full l/s/c when absent."""
    parsed = _parse_variant_tokens(name)
    return None if parsed is None else parsed[0]


def make_measurement_type(overrides: Dict[str, float], quarks: Optional[List[str]] = None) -> str:
    """Build the measurement_type string for a run-set and mass overrides.

    The set token appears only for proper subsets, before the mass tokens,
    mirroring the folder label produced by meson2pt-script.
    """
    parts = []
    if quarks is not None:
        canonical = [q for q in QUARK_ORDER if q in set(quarks)]
        if canonical and len(canonical) < 3:
            parts.append("".join(canonical))
    for k in ("ml", "ms", "mc"):
        if overrides.get(k) is not None:
            parts.append(f"{k}{overrides[k]}")
    return "meson2pt" if not parts else "meson2pt_" + "_".join(parts)


@dataclass
class Meson2ptVariant:
    """A discovered meson2pt measurement folder."""
    name: str
    dir: Path
    overrides: Dict[str, float] = field(default_factory=dict)
    quarks: List[str] = field(default_factory=lambda: list(QUARK_ORDER))


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
        parsed = _parse_variant_tokens(entry.name)
        if parsed is None:
            continue
        quarks, overrides = parsed
        variants.append(Meson2ptVariant(
            name=entry.name, dir=entry, overrides=overrides, quarks=quarks,
        ))
    variants.sort(key=lambda v: (v.name != 'meson2pt', v.name))
    return variants


class Meson2ptScanner(BaseScanner):
    """Scanner for meson 2pt measurement files (ensemble-mass or quark-subset runs).
    
    Finds files matching: {variant_dir or meson2pt}/DATA/Meson_2pt_{qq}ckn{cfg}.bin
    where {qq} is the quark pair code indexing the run-set positionally.
    
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
    
    # Reverse mapping: code to name (full l,s,c run)
    CODE_TO_MESON = {v: k for k, v in MESONS.items()}

    # Canonical quark pair to meson name
    PAIR_TO_MESON = {
        ('l', 'l'): 'pion',
        ('l', 's'): 'kaon',
        ('s', 's'): 'eta_s',
        ('l', 'c'): 'D',
        ('s', 'c'): 'Ds',
        ('c', 'c'): 'eta_c',
    }

    def __init__(self, variant_dir: Optional[Path] = None, quarks: Optional[List[str]] = None):
        """Initialize scanner.

        Args:
            variant_dir: Variant folder (e.g. meson2pt_ls_ml0.0004) whose
                DATA/ subdirectory to scan; defaults to 'meson2pt'.
            quarks: Run-set ('l','s','c' subset) the pair codes index
                positionally; defaults to the full l,s,c set.
        """
        self.variant_dir = variant_dir
        self.quarks = list(quarks) if quarks else list(QUARK_ORDER)

    def _code_to_meson(self, code: str) -> Optional[str]:
        """Map a pair code to its meson name using the run-set slot order."""
        digits = [int(ch) for ch in code]
        if any(d >= len(self.quarks) for d in digits):
            return None
        pair = tuple(sorted((self.quarks[d] for d in digits), key=QUARK_ORDER.index))
        return self.PAIR_TO_MESON.get(pair)

    
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
                
                # Convert pair code via the run-set slot order
                meson_name = self._code_to_meson(quark_code)
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
