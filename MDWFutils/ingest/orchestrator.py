"""Generic orchestrator for measurement ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from ..backends.base import DatabaseBackend
from ..parsers.base import BaseParser
from ..scanners.base import BaseScanner, ScanResult


@dataclass
class IngestResult:
    """Result from an ingestion operation."""
    ingested: int = 0
    skipped: int = 0
    errors: List[Tuple[int, str]] = None
    would_ingest: int = 0  # For dry-run mode
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MeasurementIngestor:
    """Generic orchestrator for any measurement type.
    
    Handles the common scan → filter → parse → store workflow.
    """
    
    def __init__(self, backend: DatabaseBackend, scanner: BaseScanner, parser: BaseParser, measurement_type: str):
        """Initialize ingestor.
        
        Args:
            backend: Database backend for queries and storage
            scanner: Scanner to discover files
            parser: Parser to extract data from files
            measurement_type: Measurement type identifier (e.g., "gauge_obs", "mres")
        """
        self.backend = backend
        self.scanner = scanner
        self.parser = parser
        self.measurement_type = measurement_type
    
    def ingest(
        self,
        ensemble_id: int,
        ensemble_path: Path,
        overwrite: bool = False,
        clear: bool = False,
        dry_run: bool = False,
    ) -> IngestResult:
        """Ingest measurements for an ensemble.
        
        Args:
            ensemble_id: Ensemble ID
            ensemble_path: Path to ensemble root directory
            overwrite: If True, re-parse existing configs
            clear: If True, delete all existing before ingesting
            dry_run: If True, only report what would be ingested
            
        Returns:
            IngestResult with counts and errors
        """
        # 1. Handle --clear: delete all existing before scanning
        if clear and not dry_run:
            deleted = self.backend.delete_measurements(ensemble_id, self.measurement_type)
            # Now all configs are "new"
        
        # 2. Scan for files (returns List[ScanResult])
        scan_results = self.scanner.scan(ensemble_path)
        
        # 3. Batch query existing (ONE database call)
        existing = set() if clear else set(self.backend.get_measured_configs(ensemble_id, self.measurement_type))
        
        # 4. Filter to only new configs (unless overwrite/clear)
        to_process = [r for r in scan_results if overwrite or clear or r.config_number not in existing]
        
        if dry_run:
            return IngestResult(
                would_ingest=len(to_process),
                skipped=len(scan_results) - len(to_process)
            )
        
        # 5. Parse and store (streaming - don't hold all in memory)
        ingested = 0
        errors = []
        for result in to_process:
            try:
                # Parser may modify result.metadata in place (e.g., to add source_files)
                data = self.parser.parse(result.file_path, result.metadata)
                
                # Get metadata from result (parser may have modified it)
                doc_metadata = result.metadata.get("metadata", {}).copy()
                
                self.backend.upsert_measurement(
                    ensemble_id,
                    result.config_number,
                    self.measurement_type,
                    data,
                    metadata=doc_metadata,
                )
                ingested += 1
            except Exception as e:
                errors.append((result.config_number, str(e)))
                # Skip and continue
        
        return IngestResult(
            ingested=ingested,
            skipped=len(scan_results) - len(to_process),
            errors=errors,
        )
