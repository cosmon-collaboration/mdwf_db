"""Tests for CLI help and parameter validation."""

import pytest

from MDWFutils.cli.help_generator import HelpGenerator
from MDWFutils.exceptions import ValidationError
from MDWFutils.jobs.schema import ContextParam


def test_help_generator_apply_defaults():
    schema = [
        ContextParam("AMA.NT", int, default=48),
        ContextParam("Configurations.first", int, required=True),
    ]
    typed = HelpGenerator.apply_defaults_and_validate({"Configurations.first": "0"}, schema, "input")
    assert typed["AMA.NT"] == 48
    assert typed["Configurations.first"] == 0


def test_help_generator_missing_required():
    schema = [ContextParam("Configurations.first", int, required=True)]
    with pytest.raises(ValidationError):
        HelpGenerator.apply_defaults_and_validate({}, schema, "input")


def test_format_params_detailed_includes_sections():
    input_schema = [ContextParam("AMA.NEXACT", int, default=2, help="exact count")]
    job_schema = [ContextParam("nodes", int, default=1, help="nodes")]
    text = HelpGenerator.format_params_detailed(input_schema, job_schema, "meson2pt-script")
    assert "Input parameters" in text
    assert "Job parameters" in text
    assert "AMA.NEXACT" in text
