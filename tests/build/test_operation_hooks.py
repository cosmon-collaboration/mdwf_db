"""Tests for operation logging hooks in generated build scripts."""

from MDWFutils.cli.components import BuildScriptGenerator


def test_build_script_has_running_and_exit_trap(site_ensemble_id, fake_backend, tmp_software_root):
    gen = BuildScriptGenerator(fake_backend)
    content, _ = gen.generate(
        "libxml2_gpu",
        site_ensemble_id,
        {"base": str(tmp_software_root)},
        ensemble=fake_backend.get_ensemble(site_ensemble_id),
    )
    assert '-s RUNNING' in content
    assert 'operation_id=' in content
    assert '-i "$OPERATION_ID"' in content
    assert 'trap _update_status EXIT' in content
