"""Tests for dynamic job builder registry."""

from MDWFutils.jobs.registry import (
    get_input_builder,
    get_input_schema,
    get_job_builder,
    get_job_schema,
)


def test_registry_discovers_expected_types():
    for name in ("meson2pt", "mres"):
        assert get_job_builder(name).type_name == name
    for name in ("wit_input", "hmc_xml"):
        assert get_input_builder(name).type_name == name


def test_get_job_builder_and_schema():
    builder = get_job_builder("meson2pt")
    assert builder.type_name == "meson2pt"
    job_schema, input_schema = get_job_schema("meson2pt")
    assert any(p.name == "wit_exec_path" for p in job_schema)
    assert any(p.name == "Configurations.first" for p in input_schema)


def test_get_input_builder():
    builder = get_input_builder("wit_input")
    schema = get_input_schema("wit_input")
    assert builder.type_name == "wit_input"
    assert any(p.name.startswith("AMA.") for p in schema)
