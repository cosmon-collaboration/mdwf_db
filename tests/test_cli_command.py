"""Tests for BaseCommand schema resolution."""

from MDWFutils.cli.command import _resolve_input_schema
from MDWFutils.cli.commands.meson_2pt import Meson2ptCommand
from MDWFutils.cli.commands.mres_script import MresCommand
from MDWFutils.cli.commands.wit_input import WitInputCommand


def test_resolve_input_schema_merges_job_and_input_builders():
    schema = _resolve_input_schema(Meson2ptCommand())
    names = {p.name for p in schema}
    assert "Configurations.first" in names
    assert "AMA.NEXACT" in names
    assert "Run_name.name" in names


def test_resolve_input_schema_mres_includes_ama():
    names = {p.name for p in _resolve_input_schema(MresCommand())}
    assert "AMA.NHITS" in names


def test_resolve_input_schema_input_only_command():
    schema = _resolve_input_schema(WitInputCommand())
    assert any(p.name == "Directories.cnfg_dir" for p in schema)
