"""Tests for HMC path display in status output."""

from MDWFutils.cli.commands.status import _print_ensemble_details
from MDWFutils.jobs.hmc import resolve_hmc_paths


def test_resolve_hmc_paths_honors_legacy_keys():
    resolved = resolve_hmc_paths(
        {
            "hmc_exec_path": "/legacy/bin/Nf2p1p1",
            "hmc_bind_script_gpu": "/legacy/bind_gpu.sh",
            "hmc_bind_script_cpu": "/legacy/bind_cpu.sh",
        }
    )
    assert resolved == {
        "exec_path": "/legacy/bin/Nf2p1p1",
        "bind_script_gpu": "/legacy/bind_gpu.sh",
        "bind_script_cpu": "/legacy/bind_cpu.sh",
    }


def test_resolve_hmc_paths_prefers_canonical_keys():
    resolved = resolve_hmc_paths(
        {
            "exec_path": "/canonical/bin",
            "hmc_exec_path": "/legacy/bin",
            "bind_script_gpu": "/canonical/gpu.sh",
            "hmc_bind_script_gpu": "/legacy/gpu.sh",
        }
    )
    assert resolved["exec_path"] == "/canonical/bin"
    assert resolved["bind_script_gpu"] == "/canonical/gpu.sh"


def test_status_prints_resolved_hmc_paths(capsys, fake_backend, sample_ensemble):
    fake_backend.ensembles[1]["hmc_paths"] = {
        "hmc_exec_path": "/legacy/bin/Nf2p1p1",
        "hmc_bind_script_gpu": "/legacy/bind_gpu.sh",
        "hmc_bind_script_cpu": "/legacy/bind_cpu.sh",
    }

    _print_ensemble_details(fake_backend, 1, fake_backend.ensembles[1])
    out = capsys.readouterr().out

    assert "HMC paths:" in out
    assert "exec_path       = /legacy/bin/Nf2p1p1" in out
    assert "bind_script_gpu = /legacy/bind_gpu.sh" in out
    assert "bind_script_cpu = /legacy/bind_cpu.sh" in out
