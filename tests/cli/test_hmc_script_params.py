"""Tests for HMC script -i/-j parameter classification."""

import argparse
import sys

import pytest

from MDWFutils.cli.command import resolve_command_schemas
from MDWFutils.cli.commands.hmc_script import HMCGPUCommand
from MDWFutils.cli.main import main
from MDWFutils.exceptions import ValidationError


def test_hmc_script_gpu_params_lists_run_settings_under_input(capsys):
    cmd = HMCGPUCommand()
    rc = cmd.execute(argparse.Namespace(
        params=True,
        ensemble=None,
        input_params=None,
        job_params=None,
        output_file=None,
        use_default_params=False,
        save_default_params=False,
        params_variant=None,
    ))
    assert rc == 0
    out = capsys.readouterr().out
    assert "Input parameters (-i" in out
    assert "n_trajec" in out
    assert "trajL" in out
    assert "lvl_sizes" in out
    assert "Trajectories" in out
    assert "Job parameters (-j" in out
    assert "nodes" in out
    job_section = out.split("Job parameters (-j")[1].split("Usage example:")[0]
    assert "n_trajec" not in job_section


def test_hmc_script_gpu_missing_n_trajec_reports_input_flag():
    cmd = HMCGPUCommand()
    input_schema, _ = resolve_command_schemas(cmd)
    with pytest.raises(ValidationError, match=r"pass with -i"):
        cmd.help_gen.apply_defaults_and_validate(
            {"trajL": "0.75", "lvl_sizes": "9,1,1", "Trajectories": "100"},
            input_schema,
            "input",
        )


def test_hmc_script_gpu_cli_params(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mdwf_db", "hmc-script", "gpu", "--params"])
    rc = main()
    assert rc == 0
    assert "n_trajec" in capsys.readouterr().out
