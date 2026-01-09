"""Scanner for mres measurement files."""

from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from .base import BaseScanner, ScanResult


class MresScanner(BaseScanner):
    """Scanner for unitary mres measurement files.
    
    Finds files matching:
    - Mres_{n}ckn{cfg}.bin (PP correlator, n=0,1,2 for L,S,C)
    - Mres_Mid{n}ckn{cfg}.bin (MP correlator)
    
    Returns one ScanResult per config that has all 6 required files.
    """
    
    def scan(self, ensemble_path: Path) -> List[ScanResult]:
        """Find all mres files and group by config.
        
        Args:
            ensemble_path: Path to ensemble root directory
            
        Returns:
            List of ScanResult objects, one per config with all required files
        """
        mres_dir = ensemble_path / 'mres' / 'DATA'
        if not mres_dir.exists():
            return []
        
        # Pattern: Mres_{n}ckn{cfg}.bin or Mres_Mid{n}ckn{cfg}.bin
        pp_pattern = re.compile(r'^Mres_(\d)ckn(\d+)\.bin$')
        mp_pattern = re.compile(r'^Mres_Mid(\d)ckn(\d+)\.bin$')
        
        # Group files by config number and quark index
        files_by_config: Dict[int, Dict[str, Dict[int, Path]]] = defaultdict(lambda: {'PP': {}, 'MP': {}})
        
        for file_path in mres_dir.iterdir():
            if not file_path.is_file():
                continue
            
            # Try PP pattern
            match = pp_pattern.match(file_path.name)
            if match:
                quark_idx = int(match.group(1))
                cfg_num = int(match.group(2))
                files_by_config[cfg_num]['PP'][quark_idx] = file_path
                continue
            
            # Try MP pattern
            match = mp_pattern.match(file_path.name)
            if match:
                quark_idx = int(match.group(1))
                cfg_num = int(match.group(2))
                files_by_config[cfg_num]['MP'][quark_idx] = file_path
                continue
        
        # Only return configs that have all 6 files (3 quarks × 2 types)
        results = []
        for cfg_num, files in files_by_config.items():
            pp_files = files['PP']
            mp_files = files['MP']
            
            # Check we have all 3 quarks for both PP and MP
            if set(pp_files.keys()) == {0, 1, 2} and set(mp_files.keys()) == {0, 1, 2}:
                # Store all file paths in metadata
                metadata = {
                    'files': {
                        'PP': {idx: str(pp_files[idx]) for idx in sorted(pp_files.keys())},
                        'MP': {idx: str(mp_files[idx]) for idx in sorted(mp_files.keys())},
                    },
                    'quark_indices': [0, 1, 2],
                }
                
                # Use first PP file as primary file_path
                results.append(ScanResult(
                    file_path=pp_files[0],
                    config_number=cfg_num,
                    metadata=metadata,
                ))
        
        return sorted(results, key=lambda r: r.config_number)
