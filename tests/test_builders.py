"""Integration-style tests for context builders and SLURM rendering."""

from MDWFutils.cli.components import ScriptGenerator
from MDWFutils.jobs.meson2pt import Meson2ptContextBuilder
from MDWFutils.jobs.mres import MresContextBuilder
from MDWFutils.jobs.wit import WitContextBuilder
from MDWFutils.templates.loader import TemplateLoader
from MDWFutils.templates.renderer import TemplateRenderer


def _input_params():
    return {
        "Configurations.first": 0,
        "Configurations.last": 4,
        "Configurations.step": 4,
        "AMA.NEXACT": 4,
    }


def test_meson2pt_builder_produces_slurm_context(fake_backend, sample_ensemble, tmp_ensemble_dir):
    builder = Meson2ptContextBuilder()
    context = builder.build(fake_backend, 1, job_params={}, input_params=_input_params())
    content = TemplateRenderer(TemplateLoader()).render("slurm/meson2pt.j2", context)
    assert "meson2pt" in content
    assert str(tmp_ensemble_dir) in context["workdir"]


def test_mres_builder_produces_slurm_context(fake_backend):
    builder = MresContextBuilder()
    context = builder.build(fake_backend, 1, job_params={}, input_params=_input_params())
    assert context["operation"] == "WIT_MRES"
    assert "mres" in context["workdir"]


def test_wit_input_builder_via_script_generator(fake_backend):
    generator = ScriptGenerator(fake_backend)
    content = generator.generate_input(1, "wit_input", _input_params())
    assert "[AMA]" in content
    assert "NEXACT" in content


def test_meson2pt_mass_override_changes_workdir(fake_backend, sample_ensemble):
    builder = Meson2ptContextBuilder()
    context = builder.build(
        fake_backend,
        1,
        job_params={"ml": 0.02},
        input_params=_input_params(),
    )
    assert "meson2pt_ml0.02" in context["workdir"]
