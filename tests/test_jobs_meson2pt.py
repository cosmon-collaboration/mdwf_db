"""Tests for meson2pt job helpers."""

import pytest

from MDWFutils.exceptions import ValidationError
from MDWFutils.jobs.meson2pt import (
    meson2pt_workdir_subdir,
    resolve_meson2pt_masses,
)
from MDWFutils.jobs.utils import compute_kappa
from MDWFutils.jobs.wit import WitContextBuilder, normalize_quarks


PHYSICS = {"ml": 0.0195, "ms": 0.0725, "mc": 0.8555}


def test_resolve_meson2pt_masses_defaults():
    ml, ms, mc = resolve_meson2pt_masses(PHYSICS, {})
    assert (ml, ms, mc) == (0.0195, 0.0725, 0.8555)


def test_resolve_meson2pt_masses_overrides():
    ml, ms, mc = resolve_meson2pt_masses(PHYSICS, {"ml": 0.02, "mc": 0.9})
    assert ml == 0.02
    assert ms == 0.0725
    assert mc == 0.9


def test_resolve_meson2pt_masses_missing_physics():
    with pytest.raises(ValidationError):
        resolve_meson2pt_masses({"ml": 0.01}, {})


def test_meson2pt_workdir_subdir_default():
    assert meson2pt_workdir_subdir(PHYSICS, {}) == "meson2pt"


def test_meson2pt_workdir_subdir_with_overrides():
    subdir = meson2pt_workdir_subdir(PHYSICS, {"ml": 0.02, "ms": 0.08})
    assert subdir == "meson2pt_ml0.02_ms0.08"


def test_normalize_quarks_forms():
    assert normalize_quarks(None) == ["l", "s", "c"]
    assert normalize_quarks("light,strange") == ["l", "s"]
    assert normalize_quarks("C") == ["c"]
    assert normalize_quarks("lc") == ["l", "c"]
    assert normalize_quarks("s,l") == ["l", "s"]


def test_normalize_quarks_invalid():
    for bad in ("x", "", "l,x", ","):
        with pytest.raises(ValidationError):
            normalize_quarks(bad)


def test_resolve_meson2pt_masses_rejects_override_for_unrun_quark():
    with pytest.raises(ValidationError):
        resolve_meson2pt_masses(PHYSICS, {"quarks": "l", "mc": 0.9})
    ml, ms, mc = resolve_meson2pt_masses(PHYSICS, {"quarks": "ls", "ml": 0.02})
    assert ml == 0.02


def test_meson2pt_workdir_subdir_quark_sets():
    assert meson2pt_workdir_subdir(PHYSICS, {"quarks": "light"}) == "meson2pt_l"
    assert meson2pt_workdir_subdir(PHYSICS, {"quarks": "ls", "ml": 0.004}) == "meson2pt_ls_ml0.004"
    assert meson2pt_workdir_subdir(PHYSICS, {"quarks": "lsc"}) == "meson2pt"
    assert meson2pt_workdir_subdir(PHYSICS, {"quarks": "light,strange"}) == "meson2pt_ls"


def test_wit_input_subset_renumbering(tmp_path):
    ctx = WitContextBuilder()._build_context(
        None, 1, {"directory": str(tmp_path)}, PHYSICS,
        {"quarks": "light,strange", "ml": 0.004},
        {"Configurations.first": 0, "Configurations.last": 8},
    )
    sections = {
        s["name"]: {e["key"]: e["value"] for e in s["entries"]}
        for s in ctx["sections"]
    }
    assert sections["Witness"]["no_prop"] == "2"
    assert sections["Propagator 0"]["kappa"] == str(compute_kappa(0.004))
    assert sections["Propagator 1"]["kappa"] == str(compute_kappa(PHYSICS["ms"]))
    assert "Propagator 2" not in sections
