"""Tests for ContextParam alias resolution."""

from MDWFutils.cli.command import resolve_command_schemas
from MDWFutils.cli.commands.hmc_script import HMCGPUCommand
from MDWFutils.jobs.schema import (
    ContextParam,
    collapse_schema_aliases,
    resolve_param_aliases,
)


def test_resolve_param_aliases_maps_to_canonical_name():
    schema = [
        ContextParam("Trajectories", int, aliases=["n_trajec"]),
    ]
    resolved = resolve_param_aliases({"n_trajec": 15, "mode": "reseed"}, schema)
    assert resolved["Trajectories"] == 15
    assert resolved["n_trajec"] == 15


def test_collapse_schema_aliases_keeps_job_builder_names():
    schema = [
        ContextParam("Trajectories", int, aliases=["n_trajec"]),
        ContextParam("n_trajec", int, required=True, aliases=["Trajectories"]),
        ContextParam("StartTrajectory", int, aliases=["config_start"]),
        ContextParam("config_start", int, aliases=["StartTrajectory"]),
    ]
    collapsed = collapse_schema_aliases(schema)
    names = {param.name for param in collapsed}
    assert names == {"n_trajec", "config_start"}


def test_hmc_script_merged_schema_hides_xml_only_names():
    input_schema, _ = resolve_command_schemas(HMCGPUCommand())
    names = {param.name for param in input_schema}
    assert "n_trajec" in names
    assert "Trajectories" not in names
    assert "config_start" in names
    assert "StartTrajectory" not in names
