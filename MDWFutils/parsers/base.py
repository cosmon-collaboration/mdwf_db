"""Base parser interface for extracting data from files."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict


class BaseParser(ABC):
    """Abstract base class for parsing measurement files.
    
    Parsers extract structured data from files discovered by scanners.
    """
    
    @abstractmethod
    def parse(self, file_path: Path, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Parse a file and return structured data.
        
        Args:
            file_path: Path to the file to parse
            metadata: Additional context from scanner (e.g., quark index, config number)
            
        Returns:
            Dictionary of parsed data ready for database storage
            
        Raises:
            Exception: If parsing fails (will be caught and reported by orchestrator)
        """
        pass
