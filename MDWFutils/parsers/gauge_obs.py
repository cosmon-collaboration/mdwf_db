"""Parser for gauge observable files."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .base import BaseParser


class GaugeObsParser(BaseParser):
    """Parser for t0.{cfg}.out gauge observable files.
    
    Extracts: plaq, Q, sqrt_t0_clov, sqrt_t0_plaq, w0_clov, w0_plaq
    """
    
    def parse(self, file_path: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Parse gauge observables from a t0.{cfg}.out file.
        
        Returns dict with keys: plaq, Q, sqrt_t0_clov, sqrt_t0_plaq, w0_clov, w0_plaq
        Missing values stored as float('nan').
        """
        data = {
            'plaq': float('nan'),
            'Q': float('nan'),
            'sqrt_t0_clov': float('nan'),
            'sqrt_t0_plaq': float('nan'),
            'w0_clov': float('nan'),
            'w0_plaq': float('nan'),
        }
        
        try:
            content = file_path.read_text()
            lines = content.split('\n')
            
            # Parse plaquette from "Calculated Trace" line
            for line in lines:
                if 'Calculated Trace' in line:
                    parts = line.split()
                    if parts:
                        try:
                            data['plaq'] = float(parts[-1])
                        except (ValueError, IndexError):
                            pass
                    break
            
            # Parse Q from last WFLOW line (5th-to-last word)
            wflow_lines = [line for line in lines if 'WFLOW' in line]
            if wflow_lines:
                last_wflow = wflow_lines[-1]
                parts = last_wflow.split()
                if len(parts) >= 5:
                    try:
                        data['Q'] = float(parts[-5])
                    except (ValueError, IndexError):
                        pass
            
            # Parse t0 and w0 scales (look for lines with "0.3")
            for line in lines:
                if 'GT-scale Clover' in line and '0.3' in line:
                    parts = line.split()
                    if parts:
                        try:
                            data['sqrt_t0_clov'] = float(parts[-1])
                        except (ValueError, IndexError):
                            pass
                elif 'GT-scale Plaq' in line and '0.3' in line:
                    parts = line.split()
                    if parts:
                        try:
                            data['sqrt_t0_plaq'] = float(parts[-1])
                        except (ValueError, IndexError):
                            pass
                elif 'WT-scale Clover' in line and '0.3' in line:
                    parts = line.split()
                    if parts:
                        try:
                            data['w0_clov'] = float(parts[-1])
                        except (ValueError, IndexError):
                            pass
                elif 'WT-scale Plaq' in line and '0.3' in line:
                    parts = line.split()
                    if parts:
                        try:
                            data['w0_plaq'] = float(parts[-1])
                        except (ValueError, IndexError):
                            pass
        
        except Exception:
            # Return data with NaN values if parsing fails
            pass
        
        return data
