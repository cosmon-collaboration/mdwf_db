"""Shared pytest fixtures for mdwf_db."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pytest

from MDWFutils.backends.base import DatabaseBackend
from MDWFutils.build.operations import SITE_ENSEMBLE_NICKNAME
from MDWFutils.exceptions import EnsembleNotFoundError


FIXTURES = Path(__file__).resolve().parent / "fixtures"
BUILD_FIXTURES_DIR = Path(__file__).resolve().parents[1] / "MDWFutils" / "build" / "fixtures"


def make_physics(**overrides) -> dict:
    base = {
        "beta": 4.0,
        "b": 1.75,
        "Ls": 10,
        "mc": 0.8555,
        "ms": 0.0725,
        "ml": 0.0195,
        "L": 32,
        "T": 64,
    }
    base.update(overrides)
    return base


def make_ensemble(directory: Path, ensemble_id: int = 1, **kwargs) -> dict:
    doc = {
        "ensemble_id": ensemble_id,
        "directory": str(directory.resolve()),
        "nickname": kwargs.get("nickname", "test_ensemble"),
        "status": kwargs.get("status", "TUNING"),
        "description": None,
        "physics": make_physics(**kwargs.get("physics", {})),
        "configurations": {
            "first": 0,
            "last": 100,
            "increment": 4,
            "total": 26,
            "config_list": list(range(0, 101, 4)),
            "thermalized": 20,
        },
        "hmc_paths": {},
        "grid_build": deepcopy(kwargs.get("grid_build", {})),
        "default_params": deepcopy(kwargs.get("default_params", {})),
        "tags": [],
        "notes": None,
    }
    doc.update({
        k: v
        for k, v in kwargs.items()
        if k not in ("physics", "default_params", "nickname", "status", "grid_build")
    })
    return doc


class FakeBackend(DatabaseBackend):
    """In-memory backend for unit tests."""

    def __init__(self, ensembles: Optional[Dict[int, dict]] = None):
        super().__init__("fake://test")
        self.ensembles: Dict[int, dict] = ensembles or {}
        self._next_id = max(self.ensembles.keys(), default=0) + 1
        self._next_op_id = 1
        self.default_params: Dict[tuple, dict] = {}
        self.operations: List[dict] = []
        self.measurements: Dict[tuple, dict] = {}
        self.upsert_calls: List[dict] = []

        self._rebuild_indexes()

    def _rebuild_indexes(self) -> None:
        self._by_nickname = {
            doc["nickname"]: eid
            for eid, doc in self.ensembles.items()
            if doc.get("nickname")
        }
        self._by_directory = {doc["directory"]: eid for eid, doc in self.ensembles.items()}

    def add_ensemble(self, directory: str, physics: dict, **kwargs) -> int:
        eid = self._next_id
        self._next_id += 1
        doc = make_ensemble(Path(directory), ensemble_id=eid, **kwargs)
        doc["directory"] = str(Path(directory).resolve())
        doc["physics"] = dict(physics)
        self.ensembles[eid] = doc
        self._rebuild_indexes()
        return eid

    def get_ensemble(self, ensemble_id: int) -> Optional[dict]:
        return deepcopy(self.ensembles.get(ensemble_id))

    def resolve_ensemble_identifier(self, identifier) -> Tuple[int, dict]:
        if isinstance(identifier, int) or (isinstance(identifier, str) and identifier.isdigit()):
            eid = int(identifier)
            doc = self.ensembles.get(eid)
            if doc:
                return eid, deepcopy(doc)
            raise EnsembleNotFoundError(identifier)

        ident = str(identifier)
        if ident in self._by_nickname:
            eid = self._by_nickname[ident]
            return eid, deepcopy(self.ensembles[eid])

        resolved = str(Path(ident).resolve())
        if resolved in self._by_directory:
            eid = self._by_directory[resolved]
            return eid, deepcopy(self.ensembles[eid])

        raise EnsembleNotFoundError(identifier)

    def update_ensemble(self, ensemble_id: int, **updates) -> bool:
        if ensemble_id not in self.ensembles:
            return False
        ens = self.ensembles[ensemble_id]
        for key, value in updates.items():
            if "." in key:
                top, rest = key.split(".", 1)
                ens.setdefault(top, {})
                ens[top][rest] = value
            else:
                ens[key] = value
        self._rebuild_indexes()
        return True

    def list_ensembles(self, detailed: bool = False) -> List[dict]:
        return [deepcopy(doc) for doc in self.ensembles.values()]

    def delete_ensemble(self, ensemble_id: int) -> bool:
        if ensemble_id not in self.ensembles:
            return False
        del self.ensembles[ensemble_id]
        self._rebuild_indexes()
        return True

    def get_default_params(self, ensemble_id: int, job_type: str, variant: str) -> Dict[str, str]:
        return deepcopy(
            self.default_params.get((ensemble_id, job_type, variant), {"input_params": "", "job_params": ""})
        )

    def set_default_params(
        self, ensemble_id: int, job_type: str, variant: str, input_params: str, job_params: str
    ) -> bool:
        self.default_params[(ensemble_id, job_type, variant)] = {
            "input_params": input_params,
            "job_params": job_params,
        }
        return True

    def delete_default_params(self, ensemble_id: int, job_type: str, variant: str) -> bool:
        return self.default_params.pop((ensemble_id, job_type, variant), None) is not None

    def add_operation(self, ensemble_id: int, operation_type: str, status: str, user: str, **params) -> int:
        op_id = self._next_op_id
        self._next_op_id += 1
        self.operations.append(
            {"operation_id": op_id, "ensemble_id": ensemble_id, "operation_type": operation_type, "status": status, "user": user, **params}
        )
        return op_id

    def update_operation_by_id(self, operation_id: int, status: str, **updates) -> bool:
        for op in self.operations:
            if op["operation_id"] == operation_id:
                op["status"] = status
                op.update(updates)
                return True
        return False

    def update_operation_by_slurm_id(
        self, slurm_job_id: str, status: str, ensemble_id: int, operation_type: str, **updates
    ) -> bool:
        for op in self.operations:
            if (
                op.get("slurm", {}).get("job_id") == slurm_job_id
                and op["ensemble_id"] == ensemble_id
                and op["operation_type"] == operation_type
            ):
                op["status"] = status
                op.update(updates)
                return True
        return False

    def clear_ensemble_history(self, ensemble_id: int) -> int:
        before = len(self.operations)
        self.operations = [op for op in self.operations if op["ensemble_id"] != ensemble_id]
        return before - len(self.operations)

    def list_operations(self, ensemble_id: int) -> List[dict]:
        return [deepcopy(op) for op in self.operations if op["ensemble_id"] == ensemble_id]

    def get_operation(self, ensemble_id: int, operation_id: int) -> Optional[dict]:
        for op in self.operations:
            if op["ensemble_id"] == ensemble_id and op["operation_id"] == operation_id:
                return deepcopy(op)
        return None

    def add_measurement(
        self, ensemble_id: int, config_number: int, measurement_type: str, data: dict, metadata: Optional[dict] = None
    ) -> str:
        key = (ensemble_id, config_number, measurement_type)
        self.measurements[key] = {"data": data, "metadata": metadata or {}}
        return f"m-{ensemble_id}-{config_number}-{measurement_type}"

    def query_measurements(
        self,
        ensemble_id: int,
        measurement_type: str,
        config_start: Optional[int] = None,
        config_end: Optional[int] = None,
        config_numbers: Optional[Sequence[int]] = None,
    ) -> List[dict]:
        results = []
        for (eid, cfg, mtype), payload in self.measurements.items():
            if eid != ensemble_id or mtype != measurement_type:
                continue
            if config_numbers is not None:
                if cfg not in config_numbers:
                    continue
            elif config_start is not None and cfg < config_start:
                continue
            elif config_end is not None and cfg > config_end:
                continue
            results.append({"ensemble_id": eid, "config_number": cfg, "measurement_type": mtype, **payload})
        return results

    def get_measured_configs(self, ensemble_id: int, measurement_type: str) -> List[int]:
        return sorted(
            cfg
            for (eid, cfg, mtype) in self.measurements
            if eid == ensemble_id and mtype == measurement_type
        )

    def upsert_measurement(
        self, ensemble_id: int, config_number: int, measurement_type: str, data: dict, metadata: Optional[dict] = None
    ) -> str:
        self.upsert_calls.append(
            {
                "ensemble_id": ensemble_id,
                "config_number": config_number,
                "measurement_type": measurement_type,
                "data": data,
                "metadata": metadata,
            }
        )
        return self.add_measurement(ensemble_id, config_number, measurement_type, data, metadata)

    def delete_measurements(self, ensemble_id: int, measurement_type: str) -> int:
        keys = [k for k in self.measurements if k[0] == ensemble_id and k[2] == measurement_type]
        for key in keys:
            del self.measurements[key]
        return len(keys)

    def seed_site_ensemble(self) -> int:
        for eid, doc in self.ensembles.items():
            if doc.get("nickname") == SITE_ENSEMBLE_NICKNAME:
                return eid
        return self.add_ensemble(
            "/tmp/software",
            make_physics(beta=4.0, b=1.0, Ls=4, mc=0.1, ms=0.1, ml=0.01, L=4, T=4),
            nickname=SITE_ENSEMBLE_NICKNAME,
        )

    def seed_physics_ensemble(self, physics: dict, **kwargs) -> int:
        return self.add_ensemble("/tmp/ensemble", physics, **kwargs)


@pytest.fixture
def tmp_ensemble_dir(tmp_path):
    return tmp_path / "ensemble"


@pytest.fixture
def sample_ensemble(tmp_ensemble_dir):
    tmp_ensemble_dir.mkdir(parents=True, exist_ok=True)
    return make_ensemble(tmp_ensemble_dir)


@pytest.fixture
def fake_backend(sample_ensemble):
    backend = FakeBackend({1: sample_ensemble})
    return backend


@pytest.fixture
def gauge_fixture_path():
    return FIXTURES / "gauge_obs" / "t0.100.out"


@pytest.fixture
def site_ensemble_id(fake_backend: FakeBackend) -> int:
    return fake_backend.seed_site_ensemble()


@pytest.fixture
def b4238_fixture() -> dict:
    return json.loads((BUILD_FIXTURES_DIR / "b4.238_L32.grid_build.json").read_text())


@pytest.fixture
def physics_ensemble_id(fake_backend: FakeBackend, b4238_fixture: dict) -> int:
    return fake_backend.seed_physics_ensemble(
        b4238_fixture["physics"],
        grid_build=b4238_fixture["grid_build"],
    )


@pytest.fixture
def tmp_software_root(tmp_path, monkeypatch) -> Path:
    root = tmp_path / "software"
    root.mkdir()
    monkeypatch.setenv("MDWF_SOFTWARE_ROOT", str(root))
    return root


@pytest.fixture
def mock_db(monkeypatch, fake_backend: FakeBackend):
    """Patch get_backend imports used by CLI modules."""
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")

    def _get_backend(conn):
        return fake_backend

    monkeypatch.setattr("MDWFutils.backends.get_backend", _get_backend)
    monkeypatch.setattr("MDWFutils.cli.build_command.get_backend", _get_backend)
    monkeypatch.setattr("MDWFutils.cli.ensemble_utils.get_backend", _get_backend)
    return fake_backend
