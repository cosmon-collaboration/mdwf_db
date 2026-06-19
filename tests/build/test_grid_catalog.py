"""Tests for beta-line catalog and grid_build seeding."""

import pytest

from MDWFutils.build.grid_catalog import BETA_LINES, pick_beta_line, seed_grid_build


def test_pick_beta_line_b4238():
    assert pick_beta_line(4.238) == "b4238"


def test_supported_L_for_b4238():
    spec = BETA_LINES["b4238"]
    assert 32 in spec.supported_L
    assert 24 in spec.supported_L
    assert 16 not in spec.supported_L


def test_seed_grid_build_matches_fixture(b4238_fixture):
    seeded = seed_grid_build(b4238_fixture["physics"])
    expected = b4238_fixture["grid_build"]
    assert seeded["beta_line"] == expected["beta_line"]
    assert seeded["light_mass"] == expected["light_mass"]
    assert seeded["hasenbusch"] == expected["hasenbusch"]
    assert seeded["nlvl1"] == expected["nlvl1"]


def test_seed_rejects_unsupported_L():
    physics = {"beta": 4.238, "L": 16}
    with pytest.raises(ValueError, match="not supported"):
        seed_grid_build(physics)
