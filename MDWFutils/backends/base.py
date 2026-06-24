"""Abstract database backend interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Sequence, Tuple


class DatabaseBackend(ABC):
    """Abstract interface that concrete database backends must implement."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    # ------------------------------------------------------------------
    # Ensemble operations
    # ------------------------------------------------------------------
    @abstractmethod
    def add_ensemble(self, directory: str, physics: Dict, **kwargs) -> int:
        """Add a new ensemble and return its unique ID."""

    @abstractmethod
    def get_ensemble(self, ensemble_id: int) -> Optional[Dict]:
        """Retrieve an ensemble document by ID."""

    @abstractmethod
    def resolve_ensemble_identifier(self, identifier) -> Tuple[int, Dict]:
        """Resolve ID/path/nickname to (ensemble_id, ensemble_dict)."""

    @abstractmethod
    def update_ensemble(self, ensemble_id: int, **updates) -> bool:
        """Update fields on an ensemble."""

    @abstractmethod
    def list_ensembles(self, detailed: bool = False) -> List[Dict]:
        """List ensembles, optionally including configs/metadata."""

    @abstractmethod
    def delete_ensemble(self, ensemble_id: int) -> bool:
        """Delete an ensemble and related records."""

    # ------------------------------------------------------------------
    # Legacy default parameter operations (read-only compatibility)
    # ------------------------------------------------------------------
    def get_legacy_default_params(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
    ) -> Dict[str, str]:
        """Fetch legacy nested default parameters for a command/variant.

        Returns the old string payload shape exactly as
        ``{"input_params": str, "job_params": str}``.
        Returns empty strings when no legacy defaults exist.
        """
        return {"input_params": "", "job_params": ""}

    # ------------------------------------------------------------------
    # Ensemble defaults (per-param, per-command storage)
    # ------------------------------------------------------------------
    @abstractmethod
    def get_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
    ) -> Dict[str, Dict[str, str]]:
        """Fetch per-param defaults for a command/variant.

        Returns:
            Dict with 'input_params' and 'job_params' keys, each mapping
            param name to its string value.
        """

    @abstractmethod
    def set_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
        input_params: Dict[str, str],
        job_params: Dict[str, str],
    ) -> bool:
        """Upsert per-param defaults for a command/variant."""

    @abstractmethod
    def delete_ensemble_defaults(
        self,
        ensemble_id: int,
        command: str,
        variant: str,
    ) -> bool:
        """Remove defaults for a command/variant."""

    @abstractmethod
    def list_ensemble_defaults(
        self,
        ensemble_id: int,
        command: Optional[str] = None,
    ) -> List[Dict]:
        """List all defaults for an ensemble, optionally filtered by command."""

    # ------------------------------------------------------------------
    # Operation tracking
    # ------------------------------------------------------------------
    @abstractmethod
    def add_operation(
        self,
        ensemble_id: int,
        operation_type: str,
        status: str,
        user: str,
        **params,
    ) -> int:
        """Insert a new operation record and return its ID."""

    @abstractmethod
    def update_operation_by_id(
        self,
        operation_id: int,
        status: str,
        **updates,
    ) -> bool:
        """Update an operation record by its numeric ID."""

    @abstractmethod
    def update_operation_by_slurm_id(
        self,
        slurm_job_id: str,
        status: str,
        ensemble_id: int,
        operation_type: str,
        **updates,
    ) -> bool:
        """Update an operation record by SLURM job ID, ensemble ID, and operation type."""

    @abstractmethod
    def clear_ensemble_history(self, ensemble_id: int) -> int:
        """Delete all operations for an ensemble. Returns count removed."""

    @abstractmethod
    def list_operations(self, ensemble_id: int) -> List[Dict]:
        """List operations for an ensemble."""

    @abstractmethod
    def get_operation(self, ensemble_id: int, operation_id: int) -> Optional[Dict]:
        """Get a single operation by ensemble_id and operation_id."""

    # ------------------------------------------------------------------
    # Measurement operations
    # ------------------------------------------------------------------
    @abstractmethod
    def add_measurement(
        self,
        ensemble_id: int,
        config_number: int,
        measurement_type: str,
        data: Dict,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Insert a measurement document and return its ID."""

    @abstractmethod
    def query_measurements(
        self,
        ensemble_id: int,
        measurement_type: str,
        config_start: Optional[int] = None,
        config_end: Optional[int] = None,
        config_numbers: Optional[Sequence[int]] = None,
    ) -> List[Dict]:
        """Query measurement documents by range or by explicit config list.

        If config_numbers is given, only those configs are returned (range/start/end ignored).
        """

    @abstractmethod
    def get_measured_configs(
        self,
        ensemble_id: int,
        measurement_type: str,
    ) -> List[int]:
        """Return config numbers that have measurements of given type.

        Uses distinct() for efficiency - returns only config numbers, not documents.
        """

    @abstractmethod
    def upsert_measurement(
        self,
        ensemble_id: int,
        config_number: int,
        measurement_type: str,
        data: Dict,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Insert or replace a measurement document. Returns measurement ID."""

    @abstractmethod
    def delete_measurements(
        self,
        ensemble_id: int,
        measurement_type: str,
    ) -> int:
        """Delete all measurements of given type for an ensemble.

        Returns:
            Number of documents deleted
        """
