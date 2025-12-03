"""MongoDB backend implementation."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

import time
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError, PyMongoError

from .base import DatabaseBackend
from ..exceptions import (
    ConnectionError,
    DatabaseError,
    EnsembleNotFoundError,
    ValidationError,
)
from ..schemas.validators import EnsembleCreate, PhysicsParams


def retry_on_error(max_tries: int = 3, delay: float = 1.0):
    """Decorator for retrying transient MongoDB failures."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except ConnectionFailure as exc:
                    last_exc = exc
                    if attempt == max_tries - 1:
                        raise ConnectionError(str(exc)) from exc
                    time.sleep(delay * (2**attempt))
                except PyMongoError as exc:
                    raise DatabaseError(str(exc)) from exc
            raise ConnectionError(str(last_exc)) from last_exc

        return wrapper

    return decorator


class MongoDBBackend(DatabaseBackend):
    """MongoDB-based implementation of the DatabaseBackend interface."""

    def __init__(self, connection_string: str):
        super().__init__(connection_string)

        # Connect to MongoDB
        try:
            self.client = MongoClient(
                connection_string,
                maxPoolSize=50,
                serverSelectionTimeoutMS=5000,
                retryWrites=True,
            )
            self.client.server_info()
        except ConnectionFailure as exc:
            raise ConnectionError(f"Cannot connect to MongoDB: {exc}") from exc

        self.db = self.client.get_database()
        
        self.ensembles = self.db.ensembles
        self.operations = self.db.operations
        self.measurements = self.db.measurements
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        """Create MongoDB indexes."""
        self.ensembles.create_index("directory", unique=True)
        self.ensembles.create_index("nickname", unique=True, sparse=True)
        self.ensembles.create_index("ensemble_id", unique=True)

        self.operations.create_index("ensemble_id")
        self.operations.create_index("operation_type")
        self.operations.create_index("status")
        self.operations.create_index("slurm.job_id", sparse=True)

        self.measurements.create_index(
            [
                ("ensemble_id", ASCENDING),
                ("config_number", ASCENDING),
                ("measurement_type", ASCENDING),
            ]
        )

    # ------------------------------------------------------------------
    # Ensemble operations
    # ------------------------------------------------------------------
    @retry_on_error()
    def add_ensemble(self, directory: str, physics: Dict, **kwargs) -> int:
        validated = EnsembleCreate(
            directory=directory,
            physics=PhysicsParams(**physics),
            **kwargs,
        )

        last = self.ensembles.find_one(sort=[("ensemble_id", DESCENDING)])
        next_id = (last["ensemble_id"] + 1) if last else 1

        document = {
            "ensemble_id": next_id,
            "directory": validated.directory,
            "status": validated.status,
            "description": validated.description,
            "nickname": validated.nickname,
            "physics": validated.physics.dict(),
            "configurations": {},
            "hmc_paths": {},
            "default_params": {},
            "tags": [],
            "notes": None,
            "created_at": datetime.utcnow(),
        }

        try:
            self.ensembles.insert_one(document)
        except DuplicateKeyError as exc:
            raise ValidationError(f"Ensemble already exists: {directory}") from exc

        return next_id

    @retry_on_error()
    def get_ensemble(self, ensemble_id: int) -> Optional[Dict]:
        doc = self.ensembles.find_one({"ensemble_id": ensemble_id})
        if not doc:
            return None
        doc.pop("_id", None)
        return doc

    def resolve_ensemble_identifier(self, identifier) -> tuple[int, Dict]:
        # Try integer ID
        try:
            ensemble_id = int(identifier)
            doc = self.get_ensemble(ensemble_id)
            if doc:
                return ensemble_id, doc
        except (TypeError, ValueError):
            pass

        if isinstance(identifier, str):
            from pathlib import Path

            abs_path = str(Path(identifier).expanduser().resolve())
            doc = self.ensembles.find_one({"directory": abs_path})
            if doc:
                doc.pop("_id", None)
                return doc["ensemble_id"], doc

            doc = self.ensembles.find_one({"nickname": identifier})
            if doc:
                doc.pop("_id", None)
                return doc["ensemble_id"], doc

        raise EnsembleNotFoundError(identifier)

    @retry_on_error()
    def update_ensemble(self, ensemble_id: int, **updates) -> bool:
        result = self.ensembles.update_one({"ensemble_id": ensemble_id}, {"$set": updates})
        return result.modified_count > 0

    @retry_on_error()
    def list_ensembles(self, detailed: bool = False) -> List[Dict]:
        projection = {"_id": 0}
        docs = []
        for doc in self.ensembles.find({}, projection).sort("ensemble_id", ASCENDING):
            entry = {
                "ensemble_id": doc.get("ensemble_id"),
                "directory": doc.get("directory"),
                "status": doc.get("status"),
                "description": doc.get("description"),
                "nickname": doc.get("nickname"),
            }
            if detailed:
                entry["physics"] = doc.get("physics", {})
                entry["configurations"] = doc.get("configurations", {})
                entry["hmc_paths"] = doc.get("hmc_paths", {})
                entry["tags"] = doc.get("tags", [])
                entry["default_params"] = doc.get("default_params", {})
            docs.append(entry)
        return docs

    @retry_on_error()
    def delete_ensemble(self, ensemble_id: int) -> bool:
        ens_result = self.ensembles.delete_one({"ensemble_id": ensemble_id})
        self.operations.delete_many({"ensemble_id": ensemble_id})
        self.measurements.delete_many({"ensemble_id": ensemble_id})
        return ens_result.deleted_count > 0

    # ------------------------------------------------------------------
    # Default parameter operations
    # ------------------------------------------------------------------
    @retry_on_error()
    def get_default_params(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
    ) -> Dict[str, str]:
        ensemble = self.get_ensemble(ensemble_id)
        if not ensemble:
            raise EnsembleNotFoundError(ensemble_id)

        defaults = (
            ensemble.get("default_params", {})
            .get(job_type, {})
            .get(variant, {})
        )
        return {
            "input_params": defaults.get("input_params", ""),
            "job_params": defaults.get("job_params", ""),
        }

    @retry_on_error()
    def set_default_params(
        self,
        ensemble_id: int,
        job_type: str,
        variant: str,
        input_params: str,
        job_params: str,
    ) -> bool:
        update_path = f"default_params.{job_type}.{variant}"
        result = self.ensembles.update_one(
            {"ensemble_id": ensemble_id},
            {
                "$set": {
                    f"{update_path}.input_params": input_params,
                    f"{update_path}.job_params": job_params,
                }
            },
        )
        return result.modified_count > 0

    @retry_on_error()
    def delete_default_params(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        update_path = f"default_params.{job_type}.{variant}"
        result = self.ensembles.update_one(
            {"ensemble_id": ensemble_id},
            {"$unset": {update_path: ""}},
        )
        return result.modified_count > 0

    def list_default_params(self, ensemble_id: int, job_type: Optional[str] = None) -> List[Dict]:
        """List all default parameters for an ensemble, optionally filtered by job_type."""
        ensemble = self.get_ensemble(ensemble_id)
        if not ensemble:
            raise EnsembleNotFoundError(ensemble_id)
        
        defaults = ensemble.get("default_params", {})
        result = []
        
        for jt, variants in defaults.items():
            if job_type and jt != job_type:
                continue
            for variant_name, variant_data in variants.items():
                result.append({
                    "job_type": jt,
                    "variant": variant_name,
                    "input_params": variant_data.get("input_params", ""),
                    "job_params": variant_data.get("job_params", ""),
                })
        
        return result

    # ------------------------------------------------------------------
    # Operation tracking
    # ------------------------------------------------------------------
    @retry_on_error()
    def add_operation(
        self,
        ensemble_id: int,
        operation_type: str,
        status: str,
        user: str,
        **params,
    ) -> int:
        doc = self.get_ensemble(ensemble_id)
        if not doc:
            raise EnsembleNotFoundError(ensemble_id)

        last = self.operations.find_one(sort=[("operation_id", DESCENDING)])
        next_id = (last["operation_id"] + 1) if last else 1

        operation_doc = {
            "operation_id": next_id,
            "ensemble_id": ensemble_id,
            "ensemble_directory": doc["directory"],
            "operation_type": operation_type,
            "status": status,
            "timing": {
                "creation_time": datetime.utcnow(),
                "start_time": None,
                "update_time": datetime.utcnow(),
                "end_time": None,
                "runtime_seconds": None,
            },
            "slurm": {
                "job_id": params.pop("slurm_job_id", None),
                "user": user,
                "host": params.pop("host", None),
                "batch_script": params.pop("batch_script", None),
                "output_log": params.pop("output_log", None),
                "error_log": params.pop("error_log", None),
                "exit_code": params.pop("exit_code", None),
                "slurm_status": params.pop("slurm_status", None),
            },
            "execution": {
                "run_dir": params.pop("run_dir", None),
                "config_start": params.pop("config_start", None),
                "config_end": params.pop("config_end", None),
                "config_increment": params.pop("config_increment", None),
            },
            "chain": {
                "parent_operation_id": params.pop("parent_operation_id", None),
                "attempt_number": params.pop("attempt_number", 1),
                "is_chain_member": params.pop("is_chain_member", False),
            },
            "params": params,
        }

        self.operations.insert_one(operation_doc)
        return next_id

    @retry_on_error()
    def update_operation_by_id(
        self,
        operation_id: int,
        status: str,
        **updates,
    ) -> bool:
        set_doc = {"status": status, "timing.update_time": datetime.utcnow()}
        for key, value in updates.items():
            set_doc[key] = value
        result = self.operations.update_one({"operation_id": operation_id}, {"$set": set_doc})
        return result.modified_count > 0

    @retry_on_error()
    def update_operation_by_slurm_id(
        self,
        slurm_job_id: str,
        status: str,
        ensemble_id: int,
        operation_type: str,
        **updates,
    ) -> bool:
        set_doc = {"status": status, "timing.update_time": datetime.utcnow()}
        for key, value in updates.items():
            set_doc[key] = value
        
        # Match by slurm_job_id, ensemble_id, and operation_type
        query = {
            "slurm.job_id": slurm_job_id,
            "ensemble_id": ensemble_id,
            "operation_type": operation_type
        }
        
        result = self.operations.update_one(query, {"$set": set_doc})
        return result.modified_count > 0

    @retry_on_error()
    def clear_ensemble_history(self, ensemble_id: int) -> int:
        result = self.operations.delete_many({"ensemble_id": ensemble_id})
        return result.deleted_count

    @retry_on_error()
    def list_operations(self, ensemble_id: int) -> List[Dict]:
        projection = {"_id": 0}
        rows = []
        for doc in self.operations.find({"ensemble_id": ensemble_id}, projection).sort("operation_id", ASCENDING):
            rows.append(doc)
        return rows

    @retry_on_error()
    def get_operation(self, ensemble_id: int, operation_id: int) -> Optional[Dict]:
        """Get a single operation by ensemble_id and operation_id."""
        projection = {"_id": 0}
        doc = self.operations.find_one(
            {"ensemble_id": ensemble_id, "operation_id": operation_id},
            projection
        )
        return doc

    # ------------------------------------------------------------------
    # Measurements
    # ------------------------------------------------------------------
    @retry_on_error()
    def add_measurement(
        self,
        ensemble_id: int,
        config_number: int,
        measurement_type: str,
        data: Dict,
        metadata: Optional[Dict] = None,
    ) -> str:
        document = {
            "ensemble_id": ensemble_id,
            "config_number": config_number,
            "measurement_type": measurement_type,
            "measurement_time": datetime.utcnow(),
            "data": data,
            "metadata": metadata or {},
        }
        result = self.measurements.insert_one(document)
        return str(result.inserted_id)

    @retry_on_error()
    def query_measurements(
        self,
        ensemble_id: int,
        measurement_type: str,
        config_start: Optional[int] = None,
        config_end: Optional[int] = None,
    ) -> List[Dict]:
        query = {"ensemble_id": ensemble_id, "measurement_type": measurement_type}
        if config_start is not None or config_end is not None:
            query["config_number"] = {}
            if config_start is not None:
                query["config_number"]["$gte"] = config_start
            if config_end is not None:
                query["config_number"]["$lte"] = config_end

        results = []
        for doc in self.measurements.find(query).sort("config_number", ASCENDING):
            doc["_id"] = str(doc["_id"])
            results.append(doc)
        return results


