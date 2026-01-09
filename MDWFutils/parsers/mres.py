"""Parser for mres measurement files."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import BaseParser


class MresParser(BaseParser):
    """Parser for unitary mres measurement files.
    
    Uses external creader binary to extract correlator data.
    """
    
    # Default creader path for NERSC
    NERSC_CREADER = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/build/wit_cpu/devel/extractor/Creader'
    
    def __init__(self, creader_path: Optional[str] = None, ensemble_physics: Optional[Dict[str, float]] = None):
        """Initialize parser.
        
        Args:
            creader_path: Path to creader binary (or use MDWF_CREADER_PATH env var, or NERSC default)
            ensemble_physics: Physics parameters dict with ml, ms, mc keys
        """
        self.creader_path = creader_path or os.getenv('MDWF_CREADER_PATH')
        
        # Fall back to NERSC default if it exists
        if not self.creader_path and Path(self.NERSC_CREADER).exists():
            self.creader_path = self.NERSC_CREADER
        
        if not self.creader_path:
            raise ValueError("creader path not specified. Set MDWF_CREADER_PATH environment variable.")
        
        self.ensemble_physics = ensemble_physics or {}
    
    def parse(self, file_path: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Parse mres files for a config.
        
        Args:
            file_path: Primary file path (not used directly, metadata has all paths)
            metadata: Must contain 'files' dict with PP and MP file paths.
                     This dict is modified in place to add source_files.
            
        Returns:
            Dictionary with quarks dict containing PP/MP arrays per quark
        """
        files = metadata.get('files', {})
        pp_files = files.get('PP', {})
        mp_files = files.get('MP', {})
        
        # Map quark indices to labels and masses
        quark_map = {
            0: ('light', self.ensemble_physics.get('ml', '0.0')),
            1: ('strange', self.ensemble_physics.get('ms', '0.0')),
            2: ('charm', self.ensemble_physics.get('mc', '0.0')),
        }
        
        quarks = {}
        source_files = []
        
        # Process each quark
        for quark_idx in [0, 1, 2]:
            label, mass = quark_map[quark_idx]
            
            # Parse PP file
            pp_path = Path(pp_files[quark_idx])
            pp_data = self._parse_creader_output(pp_path)
            
            # Parse MP file
            mp_path = Path(mp_files[quark_idx])
            mp_data = self._parse_creader_output(mp_path)
            
            quarks[label] = {
                'mass': str(mass),
                'PP': pp_data,
                'MP': mp_data,
            }
            
            source_files.append(pp_path.name)
            source_files.append(mp_path.name)
        
        # Add source_files to metadata (modifies in place)
        if 'metadata' not in metadata:
            metadata['metadata'] = {}
        metadata['metadata']['source_files'] = source_files
        
        return {
            'quarks': quarks,
        }
    
    def _parse_creader_output(self, file_path: Path) -> List[float]:
        """Run creader and parse CORR lines.
        
        Args:
            file_path: Path to binary file
            
        Returns:
            List of correlator values (one per time slice)
        """
        # Run: creader {file} 15,15,0,0,0 | grep CORR
        cmd = [self.creader_path, str(file_path), '15,15,0,0,0']
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )
            
            # Parse CORR lines: extract 3rd field
            correlators = []
            for line in result.stdout.split('\n'):
                if 'CORR' in line:
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            correlators.append(float(parts[2]))
                        except (ValueError, IndexError):
                            continue
            
            return correlators
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise ValueError(f"Failed to run creader on {file_path}: {e}")
