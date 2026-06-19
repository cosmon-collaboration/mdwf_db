"""Tests for mdwf_db build CLI."""

import argparse
import sys
from pathlib import Path

import pytest

from MDWFutils.cli.commands import build as build_cmd
from MDWFutils.cli.main import main


def test_build_help(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["mdwf_db", "build", "--help"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 0


def test_generate_libxml2(mock_db, site_ensemble_id, tmp_software_root, tmp_path):
    args = argparse.Namespace(
        func=build_cmd._generate_build,
        build_type="libxml2_gpu",
        ensemble="software",
        params=f"base={tmp_software_root}",
        output_file=str(tmp_path / "build_libxml2.sh"),
        force_physics_mismatch=False,
        register_paths=False,
    )
    rc = build_cmd._generate_build(args)
    assert rc == 0
    assert (tmp_path / "build_libxml2.sh").is_file()


def test_grid_init(mock_db, physics_ensemble_id, capsys):
    mock_db.ensembles[physics_ensemble_id].pop("grid_build", None)
    args = argparse.Namespace(ensemble=str(physics_ensemble_id), force=False)
    rc = build_cmd._grid_init(args)
    assert rc == 0
    updated = mock_db.get_ensemble(physics_ensemble_id)
    assert updated["grid_build"]["beta_line"] == "b4238"


def test_grid_init_skips_existing(mock_db, physics_ensemble_id, capsys):
    args = argparse.Namespace(ensemble=str(physics_ensemble_id), force=False)
    rc = build_cmd._grid_init(args)
    assert rc == 0
    assert "already exists" in capsys.readouterr().out


def test_grid_cc(mock_db, physics_ensemble_id, tmp_software_root, tmp_path):
    args = argparse.Namespace(
        func=build_cmd._generate_build,
        build_type="grid_cc",
        ensemble=str(physics_ensemble_id),
        params=f"base={tmp_software_root}",
        output_file=str(tmp_path / "Nf2p1p1.cc"),
        force_physics_mismatch=False,
        register_paths=False,
    )
    rc = build_cmd._generate_build(args)
    assert rc == 0
    assert (tmp_path / "Nf2p1p1.cc").is_file()


def test_init_site_writes_perm_fix(mock_db, tmp_software_root):
    args = argparse.Namespace()
    rc = build_cmd._init_site(args)
    assert rc == 0
    perm = tmp_software_root / "scripts" / "perm_fix_m2986.sh"
    assert perm.is_file()


def test_show_params_libxml2(capsys):
    args = argparse.Namespace(
        build_type="libxml2_gpu",
        show_params=True,
    )
    rc = build_cmd._generate_build(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "clean_install" in out
    assert "parallel_jobs" in out
