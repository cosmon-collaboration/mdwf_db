"""Tests for CLI components."""

import pytest

from MDWFutils.cli.components import ParameterManager
from MDWFutils.exceptions import EnsembleNotFoundError

from tests.conftest import FakeBackend, make_ensemble


def test_parameter_manager_parse_and_merge():
    parsed = ParameterManager.parse('nodes=2 queue=debug AMA.NEXACT=4')
    assert parsed == {"nodes": "2", "queue": "debug", "AMA.NEXACT": "4"}
    merged = ParameterManager.merge({"nodes": "1"}, {"queue": "regular"})
    assert merged == {"nodes": "1", "queue": "regular"}


def test_ensemble_resolver(fake_backend, sample_ensemble):
    from MDWFutils.cli.components import EnsembleResolver

    resolver = EnsembleResolver(fake_backend)
    eid, doc = resolver.resolve("test_ensemble")
    assert eid == 1
    assert doc["nickname"] == sample_ensemble["nickname"]

    with pytest.raises(EnsembleNotFoundError):
        resolver.resolve("missing")
