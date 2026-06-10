"""Tests for HMC bash helper emission."""

from MDWFutils.jobs.hmc_helpers import get_hmc_helpers_inline


def test_hmc_helpers_define_expected_functions():
    bash = get_hmc_helpers_inline()
    for name in (
        "hmc_find_latest_config",
        "hmc_validate_config",
        "hmc_list_configs",
    ):
        assert name in bash
    assert "ckpoint_EODWF_lat." in bash
    assert "ckpoint_EODWF_rng." in bash


def test_hmc_helpers_module_main_prints_bash():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "MDWFutils.jobs.hmc_helpers"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "hmc_find_latest_config" in result.stdout
