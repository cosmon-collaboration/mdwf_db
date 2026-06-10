"""Tests for custom exception hierarchy."""

from MDWFutils.exceptions import (
    ConnectionError,
    DatabaseError,
    EnsembleNotFoundError,
    MDWFError,
    TemplateError,
    ValidationError,
)


def test_exception_hierarchy():
    assert issubclass(EnsembleNotFoundError, MDWFError)
    assert issubclass(ValidationError, MDWFError)
    assert issubclass(ConnectionError, DatabaseError)
    assert issubclass(TemplateError, MDWFError)


def test_ensemble_not_found_message():
    err = EnsembleNotFoundError(42)
    assert "42" in str(err)
    assert err.identifier == 42
