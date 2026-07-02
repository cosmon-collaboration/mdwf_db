"""Tests for MDWFutils.jobs.utils."""

import pytest

from MDWFutils.exceptions import ValidationError
from MDWFutils.jobs.utils import (
    compute_kappa,
    ensure_keys,
    ensure_positive,
    get_physics_params,
    parse_ogeom,
    require_all,
    validate_geometry,
)

from tests.conftest import FakeBackend, make_ensemble


def test_parse_ogeom_valid():
    assert parse_ogeom("1,1,1,4") == [1, 1, 1, 4]


@pytest.mark.parametrize(
    "value",
    ["1,1,1", "0,1,1,1", "a,b,c,d"],
)
def test_parse_ogeom_invalid(value):
    with pytest.raises(ValidationError):
        parse_ogeom(value)


def test_validate_geometry_even_lgeom():
    lgeom = validate_geometry(32, 64, [1, 1, 1, 4])
    assert lgeom == [32, 32, 32, 16]


def test_validate_geometry_uneven_tiling():
    with pytest.raises(ValidationError):
        validate_geometry(33, 64, [1, 1, 1, 4])


def test_compute_kappa():
    assert compute_kappa(0.0195) == pytest.approx(1.0 / (2 * 0.0195 + 8))


def test_compute_kappa_rejects_nonpositive():
    with pytest.raises(ValidationError):
        compute_kappa(0)


def test_ensure_positive_and_keys():
    assert ensure_positive("mass", 1.0) == 1.0
    ensure_keys({"a": 1}, ["a"])
    with pytest.raises(ValidationError):
        ensure_keys({}, ["missing"])


def test_require_all_lists_missing():
    with pytest.raises(ValidationError) as exc:
        require_all({}, {"nodes": "node count"}, param_type="job")
    assert "nodes" in str(exc.value)


def test_get_physics_params_and_backend_resolution(tmp_path):
    ens = make_ensemble(tmp_path)
    backend = FakeBackend({1: ens})
    from MDWFutils.jobs.utils import get_ensemble_doc

    doc = get_ensemble_doc(backend, 1)
    assert get_physics_params(doc)["L"] == 32


def test_connection_string_backend_resolution_reuses_and_closes(monkeypatch, tmp_path):
    import MDWFutils.jobs.utils as job_utils

    class ClosableFakeBackend(FakeBackend):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.close_count = 0

        def close(self):
            self.close_count += 1

    ens = make_ensemble(tmp_path)
    backend = ClosableFakeBackend({1: ens})
    calls = []

    def get_backend(conn):
        calls.append(conn)
        return backend

    job_utils.close_cached_backends()
    monkeypatch.setattr(job_utils, "get_backend", get_backend)

    first = job_utils.get_ensemble_doc("mongodb://fake/test", 1)
    second = job_utils.get_ensemble_doc("mongodb://fake/test", 1)

    assert first["ensemble_id"] == 1
    assert second["ensemble_id"] == 1
    assert calls == ["mongodb://fake/test"]

    job_utils.close_cached_backends()
    job_utils.close_cached_backends()

    assert backend.close_count == 1
