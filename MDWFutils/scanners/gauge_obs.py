"""Scanner for gauge observable files."""

from __future__ import annotations

from pathlib import Path
from typing import List

from .base import BaseScanner, ScanResult


class GaugeObsScanner(BaseScanner):
    """Scanner for t0/*.out gauge observable files."""
    
    def scan(self, ensemble_path: Path) -> List[ScanResult]:
        """Find all t0/*.out files in the ensemble.
        
        Args:
            ensemble_path: Path to ensemble root directory
            
        Returns:
            List of ScanResult objects, one per t0 file found
        """
        results = []
        t0_dir = ensemble_path / 't0'
        
        if not t0_dir.exists():
            return results
        
        for t0_file in sorted(t0_dir.glob('t0.*.out')):
            try:
                # Extract config number from filename (t0.{cfg}.out)
                cfg_num = int(t0_file.stem.split('.')[-1])
                results.append(ScanResult(
                    file_path=t0_file,
                    config_number=cfg_num,
                    metadata={},
                ))
            except (ValueError, IndexError):
                # Skip files with invalid names
                continue
        
        return results
