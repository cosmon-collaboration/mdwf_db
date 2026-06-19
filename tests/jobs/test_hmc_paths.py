"""Tests for HMC path resolution from ensemble hmc_paths."""

from MDWFutils.jobs.hmc import HMCCPUContextBuilder, HMCGPUContextBuilder


PHYSICS = {
    "beta": 4.238,
    "b": 1.2,
    "Ls": 4,
    "mc": 0.3599,
    "ms": 0.0305,
    "ml": 0.0086,
    "L": 32,
    "T": 64,
}

INPUT_PARAMS = {
    "n_trajec": 1,
    "trajL": 1.0,
    "lvl_sizes": "9,1,1",
    "config_start": 0,
    "config_end": 10,
}

JOB_PARAMS = {
    "ntasks_per_node": 4,
}


def _ensemble(hmc_paths: dict) -> dict:
    return {"directory": "/tmp/ens", "hmc_paths": hmc_paths}


def test_gpu_reads_exec_path_and_bind_script_gpu():
    builder = HMCGPUContextBuilder()
    ctx = builder._build_context(
        None,
        1,
        _ensemble({"exec_path": "/grid/bin/Nf2p1p1", "bind_script_gpu": "/bind/gpu.sh"}),
        PHYSICS,
        JOB_PARAMS,
        INPUT_PARAMS,
    )
    assert ctx["exec_path"] == "/grid/bin/Nf2p1p1"
    assert ctx["bind_script"] == "/bind/gpu.sh"


def test_gpu_legacy_hmc_keys():
    builder = HMCGPUContextBuilder()
    ctx = builder._build_context(
        None,
        1,
        _ensemble({"hmc_exec_path": "/legacy/bin", "hmc_bind_script": "/legacy/bind.sh"}),
        PHYSICS,
        JOB_PARAMS,
        INPUT_PARAMS,
    )
    assert ctx["exec_path"] == "/legacy/bin"
    assert ctx["bind_script"] == "/legacy/bind.sh"


def test_cpu_reads_bind_script_cpu():
    builder = HMCCPUContextBuilder()
    ctx = builder._build_context(
        None,
        1,
        _ensemble({"exec_path": "/grid/bin/Nf2p1p1", "bind_script_cpu": "/bind/cpu.sh"}),
        PHYSICS,
        JOB_PARAMS,
        INPUT_PARAMS,
    )
    assert ctx["exec_path"] == "/grid/bin/Nf2p1p1"
    assert ctx["bind_script"] == "/bind/cpu.sh"
