"""In-memory fake backend for unit tests."""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from MDWFutils.backends.base import DatabaseBackend
from MDWFutils.build.operations import SITE_ENSEMBLE_NICKNAME


class FakeBackend(DatabaseBackend):
    """Minimal in-memory backend implementing the DatabaseBackend interface."""

    def __init__(self):
        super().__init__("mongodb://fake/test")
        self._ensembles: Dict[int, Dict] = {}
        self._operations: Dict[int, Dict] = {}
        self._next_ensemble_id = 1
        self._next_operation_id = 1
        self._measurements: List[Dict] = []

    def add_ensemble(self, directory: str, physics: Dict, **kwargs) -> int:
        eid = self._next_ensemble_id
        self._next_ensemble_id += 1
        doc = {
            "ensemble_id": eid,
            "directory": directory,
            "physics": physics,
            "grid_build": kwargs.pop("grid_build", {}),
            "hmc_paths": kwargs.pop("hmc_paths", {}),
            **kwargs,
        }
        self._ensembles[eid] = doc
        return eid

    def get_ensemble(self, ensemble_id: int) -> Optional[Dict]:
        return self._ensembles.get(ensemble_id)

    def resolve_ensemble_identifier(self, identifier) -> Tuple[int, Dict]:
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            eid = int(identifier)
            ens = self._ensembles.get(eid)
            if ens:
                return eid, ens
        if isinstance(identifier, str):
            for eid, ens in self._ensembles.items():
                if ens.get("nickname") == identifier:
                    return eid, ens
                if ens.get("directory") == identifier:
                    return eid, ens
        raise KeyError(f"Ensemble not found: {identifier}")

    def update_ensemble(self, ensemble_id: int, **updates) -> bool:
        ens = self._ensembles.get(ensemble_id)
        if not ens:
            return False
        for key, value in updates.items():
            if "." in key:
                top, rest = key.split(".", 1)
                ens.setdefault(top, {})
                ens[top][rest] = value
            else:
                ens[key] = value
        return True

    def list_ensembles(self, detailed: bool = False) -> List[Dict]:
        return list(self._ensembles.values())

    def delete_ensemble(self, ensemble_id: int) -> bool:
        return self._ensembles.pop(ensemble_id, None) is not None

    def get_default_params(self, ensemble_id: int, job_type: str, variant: str) -> Dict[str, str]:
        ens = self._ensembles.get(ensemble_id, {})
        return ens.get("default_params", {}).get(job_type, {}).get(variant, {})

    def set_default_params(
        self, ensemble_id: int, job_type: str, variant: str, input_params: str, job_params: str
    ) -> bool:
        ens = self._ensembles.setdefault(ensemble_id, {})
        ens.setdefault("default_params", {}).setdefault(job_type, {})[variant] = {
            "input_params": input_params,
            "job_params": job_params,
        }
        return True

    def delete_default_params(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        ens = self._ensembles.get(ensemble_id, {})
        variants = ens.get("default_params", {}).get(job_type, {})
        return variants.pop(variant, None) is not None

    def add_operation(
        self, ensemble_id: int, operation_type: str, status: str, user: str, **params
    ) -> int:
        oid = self._next_operation_id
        self._next_operation_id += 1
        self._operations[oid] = {
            "operation_id": oid,
            "ensemble_id": ensemble_id,
            "operation_type": operation_type,
            "status": status,
            "user": user,
            "params": params,
        }
        return oid

    def update_operation_by_id(self, operation_id: int, status: str, **updates) -> bool:
        op = self._operations.get(operation_id)
        if not op:
            return False
        op["status"] = status
        op.setdefault("updates", []).append(updates)
        return True

    def update_operation_by_slurm_id(
        self, slurm_job_id: str, status: str, ensemble_id: int, operation_type: str, **updates
    ) -> bool:
        return False

    def clear_ensemble_history(self, ensemble_id: int) -> int:
        to_remove = [oid for oid, op in self._operations.items() if op["ensemble_id"] == ensemble_id]
        for oid in to_remove:
            del self._operations[oid]
        return len(to_remove)

    def list_operations(self, ensemble_id: int) -> List[Dict]:
        return [op for op in self._operations.values() if op["ensemble_id"] == ensemble_id]

    def get_operation(self, ensemble_id: int, operation_id: int) -> Optional[Dict]:
        op = self._operations.get(operation_id)
        if op and op["ensemble_id"] == ensemble_id:
            return op
        return None

    def add_measurement(
        self, ensemble_id: int, config_number: int, measurement_type: str, data: Dict, metadata=None
    ) -> str:
        mid = f"m-{len(self._measurements)}"
        self._measurements.append(
            {
                "id": mid,
                "ensemble_id": ensemble_id,
                "config_number": config_number,
                "measurement_type": measurement_type,
                "data": data,
            }
        )
        return mid

    def query_measurements(self, *args, **kwargs) -> List[Dict]:
        return []

    def get_measured_configs(self, ensemble_id: int, measurement_type: str) -> List[int]:
        return []

    def upsert_measurement(self, *args, **kwargs) -> str:
        return "upserted"

    def delete_measurements(self, ensemble_id: int, measurement_type: str) -> int:
        return 0

    def seed_site_ensemble(self) -> int:
        return self.add_ensemble(
            "/tmp/software",
            {"beta": 4.0, "b": 1.0, "Ls": 4, "mc": 0.1, "ms": 0.1, "ml": 0.01, "L": 4, "T": 4},
            nickname=SITE_ENSEMBLE_NICKNAME,
        )

    def seed_physics_ensemble(self, physics: Dict, **kwargs) -> int:
        return self.add_ensemble("/tmp/ensemble", physics, **kwargs)
