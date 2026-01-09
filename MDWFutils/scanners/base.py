"""Base scanner interface for file discovery."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class ScanResult:
    """Result from scanning for measurement files.
    
    Attributes:
        file_path: Path to the file (or primary file if multiple files per config)
        config_number: Configuration number this measurement is for
        metadata: Additional information (e.g., quark index, file patterns)
    """
    file_path: Path
    config_number: int
    metadata: Dict[str, Any]


class BaseScanner(ABC):
    """Abstract base class for discovering measurement files.
    
    Scanners are responsible for finding files and extracting metadata,
    but do NOT parse file contents.
    """
    
    @abstractmethod
    def scan(self, ensemble_path: Path) -> List[ScanResult]:
        """Scan for measurement files in an ensemble directory.
        
        Args:
            ensemble_path: Path to ensemble root directory
            
        Returns:
            List of ScanResult objects, one per config that has complete files
        """
        pass
