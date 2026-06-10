"""Tests for meson2pt job helpers."""

import pytest

from MDWFutils.exceptions import ValidationError
from MDWFutils.jobs.meson2pt import (
    meson2pt_workdir_subdir,
    resolve_meson2pt_masses,
)


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
