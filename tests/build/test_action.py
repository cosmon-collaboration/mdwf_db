"""Tests for action string formatting."""

from MDWFutils.build.action import format_action, grid_hmc_exec_path, grid_install_prefix
from MDWFutils.jobs.hmc import _format_ensemble_name


def test_format_action_matches_hmc(b4238_fixture):
    physics = b4238_fixture["physics"]
    assert format_action(physics) == _format_ensemble_name(physics)
    assert format_action(physics) == b4238_fixture["action"]


def test_grid_paths():
    prefix = grid_install_prefix("/install/gpu", "b4.238_action")
    assert prefix == "/install/gpu/Grid_b4.238_action"
    assert grid_hmc_exec_path(prefix) == "/install/gpu/Grid_b4.238_action/bin/Nf2p1p1"
