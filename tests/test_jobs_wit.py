"""Tests for WIT input builder and AMA overrides."""

from MDWFutils.cli.command import _resolve_input_schema
from MDWFutils.cli.commands.meson_2pt import Meson2ptCommand
from MDWFutils.cli.help_generator import HelpGenerator
from MDWFutils.jobs.wit import (
    DEFAULT_AMA_PARAMS,
    WitContextBuilder,
    _build_parameters,
    _unflatten_params,
    common_wit_ama_params,
    update_nested_dict,
)
from MDWFutils.templates.loader import TemplateLoader
from MDWFutils.templates.renderer import TemplateRenderer

from tests.conftest import make_ensemble, make_physics


def test_common_wit_ama_params_defaults_match_template():
    params = {p.name: p.default for p in common_wit_ama_params()}
    assert str(params["AMA.NEXACT"]) == DEFAULT_AMA_PARAMS["NEXACT"]
    assert params["AMA.SLOPPY_PREC"] == DEFAULT_AMA_PARAMS["SLOPPY_PREC"]


def test_unflatten_dotted_keys():
    nested = _unflatten_params({"AMA.NEXACT": 4, "Configurations.first": 0})
    assert nested["AMA"]["NEXACT"] == 4
    assert nested["Configurations"]["first"] == 0


def test_build_parameters_applies_ama_overrides():
    overrides = _unflatten_params({"AMA.NEXACT": 4, "AMA.NHITS": 3})
    params = _build_parameters(make_physics(), overrides)
    assert params["AMA"]["NEXACT"] == 4
    assert params["AMA"]["NHITS"] == 3
    assert params["AMA"]["SLOPPY_PREC"] == DEFAULT_AMA_PARAMS["SLOPPY_PREC"]


def test_update_nested_dict_merges_sections():
    target = {"AMA": {"NEXACT": "2"}}
    update_nested_dict(target, {"AMA": {"NHITS": "5"}})
    assert target["AMA"] == {"NEXACT": "2", "NHITS": "5"}


def test_meson2pt_merged_schema_includes_ama():
    schema = _resolve_input_schema(Meson2ptCommand())
    names = {p.name for p in schema}
    assert "Configurations.first" in names
    assert "AMA.NEXACT" in names


def test_wit_context_builder_renders_ama_section(tmp_path):
    ensemble = make_ensemble(tmp_path)
    schema = _resolve_input_schema(Meson2ptCommand())
    typed_input = HelpGenerator.apply_defaults_and_validate(
        {
            "Configurations.first": "0",
            "Configurations.last": "4",
            "AMA.NEXACT": "4",
            "AMA.NHITS": "3",
            "AMA.SLOPPY_PREC": "1E-6",
            "AMA.NT": "32",
        },
        schema,
        "input",
    )
    builder = WitContextBuilder()
    context = builder._build_context(
        backend=None,
        ensemble_id=1,
        ensemble=ensemble,
        physics=ensemble["physics"],
        job_params={},
        input_params=typed_input,
    )
    content = TemplateRenderer(TemplateLoader()).render("input/wit_input.j2", context)
    assert "[AMA]" in content
    assert "NEXACT       4" in content
    assert "NHITS        3" in content
    assert "SLOPPY_PREC  1E-6" in content
    assert "NT           32" in content
