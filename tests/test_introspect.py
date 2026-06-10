"""Tests for CLI introspection helpers."""

from MDWFutils.cli.introspect import get_command_metadata


def test_get_command_metadata_for_mres():
    commands = get_command_metadata()
    meta = commands["mres-script"]
    assert meta["type"] == "base_command"
    input_names = {p["name"] for p in meta["input_params"]}
    job_names = {p["name"] for p in meta["job_params"]}
    assert "AMA.NEXACT" in input_names
    assert "wit_exec_path" in job_names
