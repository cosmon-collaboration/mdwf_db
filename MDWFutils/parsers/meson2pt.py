"""Parser for meson 2pt measurement files."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import BaseParser


class Meson2ptParser(BaseParser):
    """Parser for unitary meson 2pt measurement files.
    
    Uses external creader binary to extract PP and AP correlator data.
    """
    
    # Default creader path for NERSC
    NERSC_CREADER = '/global/cfs/cdirs/m2986/cosmon/mdwf/software/build/wit_cpu/devel/extractor/Creader'
    
    def __init__(self, creader_path: Optional[str] = None):
        """Initialize parser.
        
        Args:
            creader_path: Path to creader binary (or use MDWF_CREADER_PATH env var, or NERSC default)
        """
        self.creader_path = creader_path or os.getenv('MDWF_CREADER_PATH')
        
        # Fall back to NERSC default if it exists
        if not self.creader_path and Path(self.NERSC_CREADER).exists():
            self.creader_path = self.NERSC_CREADER
        
        if not self.creader_path:
            raise ValueError("creader path not specified. Set MDWF_CREADER_PATH environment variable.")
    
    def parse(self, file_path: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Parse meson2pt files for a config.
        
        Args:
            file_path: Primary file path (not used directly, metadata has all paths)
            metadata: Must contain 'files' dict mapping meson names to file paths.
                     This dict is modified in place to add source_files.
            
        Returns:
            Dictionary with mesons dict containing PP/AP arrays per meson
        """
        files = metadata.get('files', {})
        
        mesons = {}
        source_files = []
        
        # Process each available meson
        for meson_name, meson_path in files.items():
            meson_path = Path(meson_path)
            
            # PP correlator: gamma indices 15,15
            pp_data = self._parse_creader_output(meson_path, '15,15,0,0,0')
            
            # AP correlator: gamma indices 7,15
            ap_data = self._parse_creader_output(meson_path, '7,15,0,0,0')
            
            mesons[meson_name] = {
                'PP': pp_data,
                'AP': ap_data,
            }
            
            source_files.append(meson_path.name)
        
        # Add source_files to metadata (modifies in place)
        if 'metadata' not in metadata:
            metadata['metadata'] = {}
        metadata['metadata']['source_files'] = source_files
        
        return {
            'mesons': mesons,
        }
    
    def _parse_creader_output(self, file_path: Path, gamma_indices: str) -> List[float]:
        """Run creader and parse CORR lines.
        
        Args:
            file_path: Path to binary file
            gamma_indices: Gamma matrix indices string (e.g., '15,15,0,0,0' or '7,15,0,0,0')
            
        Returns:
            List of correlator values (one per time slice)
        """
        cmd = [self.creader_path, str(file_path), gamma_indices]
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
