"""Tests for build context builders."""

from pathlib import Path

import pytest

from MDWFutils.build.registry import get_build_builder
from MDWFutils.exceptions import ValidationError


def test_libxml2_context_keys(site_ensemble_id, fake_backend, tmp_software_root):
    builder = get_build_builder("libxml2_gpu")
    ctx = builder.build(
        fake_backend,
        site_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(site_ensemble_id),
    )
    assert ctx["pkg"] == "libxml2"
    assert ctx["install_prefix"] == f"{tmp_software_root}/install_gpu/libxml2"
    assert ctx["_executable"] is True


def test_grid_hmc_gpu_fails_without_cc(fake_backend, physics_ensemble_id, tmp_software_root):
    builder = get_build_builder("grid_hmc_gpu")
    with pytest.raises(ValidationError, match="Missing"):
        builder.build(
            fake_backend,
            physics_ensemble_id,
            {"base": str(tmp_software_root)},
            ensemble=fake_backend.get_ensemble(physics_ensemble_id),
        )


def test_grid_hmc_gpu_succeeds_with_cc(
    fake_backend, physics_ensemble_id, b4238_fixture, tmp_software_root
):
    from MDWFutils.cli.build_command import write_build_artifact
    from MDWFutils.cli.components import BuildScriptGenerator

    gen = BuildScriptGenerator(fake_backend)
    content, ctx = gen.generate(
        "grid_cc",
        physics_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(physics_ensemble_id),
    )
    write_build_artifact(content, ctx)
    action = b4238_fixture["action"]
    cc_path = tmp_software_root / "scripts" / "grid_scripts" / f"Nf2p1p1_{action}.cc"
    assert cc_path.is_file()

    builder = get_build_builder("grid_hmc_gpu")
    ctx = builder.build(
        fake_backend,
        physics_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(physics_ensemble_id),
    )
    assert ctx["hmc_exec_path"].endswith("/bin/Nf2p1p1")
    assert Path(ctx["nf2p1p1_src"]).is_file()
