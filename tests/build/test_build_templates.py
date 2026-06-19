"""Tests for build shell template rendering."""

import pytest

from MDWFutils.build.registry import _builder_map
from MDWFutils.cli.components import BuildScriptGenerator


SHELL_BUILD_TYPES = [
    "libxml2_gpu",
    "qmp_gpu",
    "qdpxx_gpu",
    "quda_gpu",
    "wit_gpu",
    "wit_stack",
    "glu_cpu",
]


@pytest.mark.parametrize("build_type", SHELL_BUILD_TYPES)
def test_shell_template_renders(site_ensemble_id, fake_backend, build_type, tmp_software_root):
    gen = BuildScriptGenerator(fake_backend)
    content, ctx = gen.generate(
        build_type,
        site_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(site_ensemble_id),
    )
    assert "mdwf_db update" in content
    assert "OPERATION_ID" in content
    assert ctx.get("install_prefix") or build_type == "wit_stack"


def test_qdpxx_uses_install_qmp_path(site_ensemble_id, fake_backend, tmp_software_root):
    gen = BuildScriptGenerator(fake_backend)
    content, _ = gen.generate(
        "qdpxx_gpu",
        site_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(site_ensemble_id),
    )
    assert "--with-qmp=${INSTALL_DIR}/qmp" in content


def test_all_builders_registered():
    builders = _builder_map()
    assert "grid_cc" in builders
    assert "grid_hmc_gpu" in builders
    assert len(builders) >= 10
