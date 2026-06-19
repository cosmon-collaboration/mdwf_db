"""Tests for SiteProfile resolution."""

import pytest

from MDWFutils.build.site import DEFAULT_SOFTWARE_ROOT, resolve_site_profile


def test_default_software_root():
    profile = resolve_site_profile()
    assert profile.base == DEFAULT_SOFTWARE_ROOT
    assert profile.scripts_dir == f"{DEFAULT_SOFTWARE_ROOT}/scripts"


def test_env_override(monkeypatch):
    monkeypatch.setenv("MDWF_SOFTWARE_ROOT", "/custom/root")
    profile = resolve_site_profile()
    assert profile.base == "/custom/root"
    assert profile.install_gpu_dir == "/custom/root/install_gpu"


def test_param_override():
    profile = resolve_site_profile({"base": "/override", "gpu_arch": "sm_90"})
    assert profile.base == "/override"
    assert profile.gpu_arch == "sm_90"


def test_unknown_override_raises():
    with pytest.raises(ValueError, match="Unknown site profile"):
        resolve_site_profile({"not_a_field": "x"})
