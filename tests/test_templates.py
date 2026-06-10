"""Template loader and renderer tests."""

from MDWFutils.templates.loader import TemplateLoader
from MDWFutils.templates.renderer import TemplateRenderer


def test_render_wit_input_template():
    context = {
        "sections": [
            {
                "name": "AMA",
                "entries": [
                    {"key": "NEXACT", "value": "4"},
                    {"key": "NHITS", "value": "3"},
                ],
            }
        ]
    }
    content = TemplateRenderer(TemplateLoader()).render("input/wit_input.j2", context)
    assert "[AMA]" in content
    assert "NEXACT       4" in content


def test_render_slurm_meson2pt_template():
    context = {
        "log_dir": "/tmp/jlog",
        "separate_error_log": False,
        "job_name": "meson2pt_test",
        "ensemble_id": 1,
        "operation": "WIT_MESON2PT",
        "run_dir": "/tmp/ensemble",
        "params": "kappaL=0.1 kappaS=0.2 kappaC=0.3",
        "workdir": "/tmp/ensemble/meson2pt",
        "env_setup": "true",
        "wit_input_path": "/tmp/ensemble/meson2pt/DWF_meson2pt.in",
        "ogeom": "1 1 1 4",
        "lgeom": "32 32 32 16",
        "config_start": 0,
        "config_end": 4,
        "config_inc": 4,
    }
    content = TemplateRenderer(TemplateLoader()).render("slurm/meson2pt.j2", context)
    assert "#SBATCH" in content
    assert "meson2pt_test" in content
    assert "DWF_meson2pt.in" in content
