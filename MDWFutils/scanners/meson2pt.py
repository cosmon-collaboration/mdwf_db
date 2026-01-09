"""Scanner for meson 2pt measurement files."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from .base import BaseScanner, ScanResult


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
    
    def scan(self, ensemble_path: Path) -> List[ScanResult]:
        """Find all meson2pt files and group by config.
        
        Args:
            ensemble_path: Path to ensemble root directory
            
        Returns:
            List of ScanResult objects, one per config with at least one meson file
        """
        meson_dir = ensemble_path / 'meson2pt' / 'DATA'
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
