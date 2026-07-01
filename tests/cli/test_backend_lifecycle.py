"""Regression tests for CLI Mongo backend lifecycle."""

from __future__ import annotations

from types import SimpleNamespace

from MDWFutils.cli import runtime
from MDWFutils.cli.commands import init_db, status, update
from tests.conftest import FakeBackend, make_ensemble


class ClosableFakeBackend(FakeBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.close_count = 0
        self.validate_count = 0
        self.ensure_indexes_count = 0

    def close(self) -> None:
        self.close_count += 1

    def validate_connection(self) -> None:
        self.validate_count += 1

    def ensure_indexes(self) -> None:
        self.ensure_indexes_count += 1


def test_load_default_backend_reuses_and_closes_cached_backend(monkeypatch):
    backend = ClosableFakeBackend()
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    first = runtime.load_default_backend()
    second = runtime.load_default_backend()

    assert first is backend
    assert second is backend
    assert calls == [("mongodb://fake/test", False, False)]

    runtime.close_default_backends()
    runtime.close_default_backends()

    assert backend.close_count == 1


def test_cached_backend_can_be_upgraded_for_validation_and_indexes(monkeypatch):
    backend = ClosableFakeBackend()
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    first = runtime.load_default_backend()
    second = runtime.load_default_backend(validate_connection=True, ensure_indexes=True)
    third = runtime.load_default_backend(validate_connection=True, ensure_indexes=True)

    assert first is backend
    assert second is backend
    assert third is backend
    assert calls == [("mongodb://fake/test", False, False)]
    assert backend.validate_count == 1
    assert backend.ensure_indexes_count == 1

    runtime.close_default_backends()


def test_init_style_backend_load_can_request_validation_and_indexes(monkeypatch):
    backend = ClosableFakeBackend()
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    loaded = runtime.load_default_backend(validate_connection=True, ensure_indexes=True)

    assert loaded is backend
    assert calls == [("mongodb://fake/test", True, True)]

    runtime.close_default_backends()


def test_init_db_requests_validation_and_indexes(monkeypatch, tmp_path):
    backend = ClosableFakeBackend()
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    args = SimpleNamespace(base_dir=str(tmp_path))

    assert init_db.do_init(args) == 0
    assert calls == [("mongodb://fake/test", True, True)]

    runtime.close_default_backends()


def test_update_command_uses_one_backend_instance(monkeypatch, tmp_path):
    backend = ClosableFakeBackend({1: make_ensemble(tmp_path)})
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    args = SimpleNamespace(
        ensemble="1",
        operation_type="MRES",
        status="RUNNING",
        operation_id=None,
        params="slurm_job_id=123",
        user="tester",
    )

    assert update.do_update(args) == 0
    assert len(calls) == 1

    runtime.close_default_backends()


def test_status_detail_command_uses_one_backend_instance(monkeypatch, tmp_path, capsys):
    backend = ClosableFakeBackend({1: make_ensemble(tmp_path)})
    calls = []

    def get_backend(conn, *, validate_connection=True, ensure_indexes=True):
        calls.append((conn, validate_connection, ensure_indexes))
        return backend

    runtime.close_default_backends()
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(runtime, "get_backend", get_backend)

    args = SimpleNamespace(
        ensemble=["1"],
        measurements=None,
        op=None,
        missing=None,
        measured=None,
        cfg_range=None,
        dir=True,
        sort_by_id=False,
    )

    assert status.do_status(args) == 0
    assert str(tmp_path.resolve()) in capsys.readouterr().out
    assert len(calls) == 1

    runtime.close_default_backends()
