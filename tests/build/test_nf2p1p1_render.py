"""Tests for Nf2p1p1.cc rendering."""

from MDWFutils.cli.components import BuildScriptGenerator


def test_render_nf2p1p1_from_fixture(fake_backend, physics_ensemble_id, b4238_fixture, tmp_software_root):
    gen = BuildScriptGenerator(fake_backend)
    content, ctx = gen.generate(
        "grid_cc",
        physics_ensemble_id,
        {},
        ensemble=fake_backend.get_ensemble(physics_ensemble_id),
        command_line="mdwf_db build grid cc",
    )
    assert b4238_fixture["action"] in content
    assert "0.0086" in content
    assert "0.035" in content
    assert "#ifdef" not in content
    assert ctx["_output_suffix"] == ".cc"
    assert f"Nf2p1p1_{b4238_fixture['action']}.cc" in str(ctx["_output_prefix"] + ctx["_output_suffix"])
