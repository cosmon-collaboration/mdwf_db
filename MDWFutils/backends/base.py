"""Abstract database backend interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple


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
    # Default parameter operations
    # ------------------------------------------------------------------
    @abstractmethod
    def get_default_params(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
    ) -> Dict[str, str]:
        """Fetch default parameters for a job type/variant."""

    @abstractmethod
    def set_default_params(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
        input_params: str,
        job_params: str,
    ) -> bool:
        """Persist default parameter strings for a job type/variant."""

    @abstractmethod
    def delete_default_params(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        """Remove stored default params for a job type/variant."""

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
        **updates,
    ) -> bool:
        """Update an operation record by SLURM job ID."""

    @abstractmethod
    def clear_ensemble_history(self, ensemble_id: int) -> int:
        """Delete all operations for an ensemble. Returns count removed."""

    @abstractmethod
    def list_operations(self, ensemble_id: int) -> List[Dict]:
        """List operations for an ensemble."""

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
    ) -> List[Dict]:
        """Query measurement documents for a range of configurations."""


