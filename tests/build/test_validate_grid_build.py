"""Tests for grid_build validation."""

import pytest

from MDWFutils.build.validate_grid_build import merge_physics_and_grid_build, validate_grid_build
from MDWFutils.exceptions import ValidationError


def test_validate_passes_for_fixture(b4238_fixture):
    warnings = validate_grid_build(b4238_fixture["physics"], b4238_fixture["grid_build"])
    assert warnings == []


def test_missing_grid_build_raises():
    with pytest.raises(ValidationError, match="grid_build is missing"):
        validate_grid_build({"beta": 4.238}, {})


def test_physics_mismatch_raises_without_force(b4238_fixture):
    physics = dict(b4238_fixture["physics"])
    physics["beta"] = 4.0
    with pytest.raises(ValidationError, match="force-physics-mismatch"):
        validate_grid_build(physics, b4238_fixture["grid_build"])


def test_physics_mismatch_warns_with_force(b4238_fixture):
    physics = dict(b4238_fixture["physics"])
    physics["beta"] = 4.0
    warnings = validate_grid_build(
        physics, b4238_fixture["grid_build"], force_physics_mismatch=True
    )
    assert any("physics.beta" in w for w in warnings)


def test_merge_context(b4238_fixture):
    merged = merge_physics_and_grid_build(b4238_fixture["physics"], b4238_fixture["grid_build"])
    assert merged["light_mass"] == 0.0086
    assert merged["hasenbusch"] == [0.035, 0.14, 0.4]
    assert merged["L"] == 32
