"""Tests for HMC XML generation and -i override wiring."""

import argparse
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from MDWFutils.cli.commands.hmc_script import HMCGPUCommand
from MDWFutils.cli.commands.hmc_xml import HMCXMLCommand
from MDWFutils.exceptions import ValidationError
from MDWFutils.jobs.hmc import (
    HMCXMLContextBuilder,
    _apply_xml_overrides,
    _make_default_tree,
)


def _hmc_params(xml: str) -> ET.Element:
    root = ET.fromstring(xml)
    hmc = root.find("HMCparameters")
    assert hmc is not None
    return hmc


def _hmc_vals(xml: str) -> dict:
    hmc = _hmc_params(xml)
    return {
        "Trajectories": hmc.find("Trajectories").text,
        "StartTrajectory": hmc.find("StartTrajectory").text,
        "trajL": hmc.find("MD/trajL").text,
        "MDsteps": hmc.find("MD/MDsteps").text,
        "lvl_sizes": [elem.text for elem in hmc.findall("MD/lvl_sizes/elem")],
        "md_name": [elem.text for elem in hmc.findall("MD/name/elem")],
    }


def _run_hmc_command(cmd, ens_dir, *, input_params, job_params=None):
    rc = cmd.execute(
        argparse.Namespace(
            params=False,
            ensemble=str(ens_dir),
            input_params=input_params,
            job_params=job_params,
            output_file=None,
            use_default_params=False,
            save_default_params=False,
            params_variant=None,
        )
    )
    assert rc == 0
    return ens_dir / "cnfg" / "HMCparameters.xml"


@pytest.fixture
def hmc_backend(sample_ensemble, fake_backend):
    fake_backend.ensembles[1]["hmc_paths"] = {
        "exec_path": "/bin/Nf2p1p1",
        "bind_script_gpu": "/bind/gpu.sh",
    }
    return fake_backend


@pytest.fixture
def hmc_ens_dir(sample_ensemble):
    return Path(sample_ensemble["directory"])


def test_apply_xml_overrides_updates_hmcparameters_children():
    tree, root = _make_default_tree("reseed", 42)
    _apply_xml_overrides(
        root,
        {
            "Trajectories": 15,
            "trajL": 2.6,
            "StartTrajectory": 0,
            "lvl_sizes": "9,1,1",
        },
    )
    hmc = root.find("HMCparameters")
    assert hmc.find("Trajectories").text == "15"
    assert hmc.find("StartTrajectory").text == "0"
    assert hmc.find("MD/trajL").text == "2.6"
    assert [elem.text for elem in hmc.findall("MD/lvl_sizes/elem")] == ["9", "1", "1"]


def test_hmc_xml_builder_maps_n_trajec_and_config_start(fake_backend, sample_ensemble):
    builder = HMCXMLContextBuilder()
    ctx = builder.build(
        fake_backend,
        1,
        input_params={
            "mode": "reseed",
            "n_trajec": 15,
            "trajL": 2.6,
            "config_start": 0,
            "lvl_sizes": "9,1,1",
        },
    )
    hmc = _hmc_params(ctx["xml"])
    assert hmc.find("Trajectories").text == "15"
    assert hmc.find("StartTrajectory").text == "0"
    assert hmc.find("MD/trajL").text == "2.6"
    assert ctx["_output_suffix"] == ".xml"


def test_hmc_xml_builder_requires_trajectory_count(fake_backend):
    builder = HMCXMLContextBuilder()
    with pytest.raises(ValidationError, match="Trajectories"):
        builder.build(
            fake_backend,
            1,
            input_params={"mode": "reseed", "trajL": 1.0},
        )


def test_hmc_script_gpu_writes_xml_with_overrides(hmc_ens_dir, hmc_backend, monkeypatch):
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(
        "MDWFutils.cli.runtime.get_backend",
        lambda _url: hmc_backend,
    )

    xml_path = _run_hmc_command(
        HMCGPUCommand(backend=hmc_backend),
        hmc_ens_dir,
        input_params="n_trajec=15 trajL=2.6 mode=reseed config_start=0 lvl_sizes=9,1,1",
        job_params="exec_path=/bin/Nf2p1p1 bind_script=/bind/gpu.sh",
    )
    assert xml_path.is_file()
    assert not (hmc_ens_dir / "cnfg" / "HMCparameters.in").exists()

    vals = _hmc_vals(xml_path.read_text())
    assert vals == {
        "Trajectories": "15",
        "StartTrajectory": "0",
        "trajL": "2.6",
        "MDsteps": "1",
        "lvl_sizes": ["9", "1", "1"],
        "md_name": ["OMF2_5StepV", "OMF2_5StepV", "OMF4_11StepV"],
    }


def test_hmc_script_accepts_xml_alias_names_and_optional_overrides(hmc_ens_dir, hmc_backend, monkeypatch):
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(
        "MDWFutils.cli.runtime.get_backend",
        lambda _url: hmc_backend,
    )

    xml_path = _run_hmc_command(
        HMCGPUCommand(backend=hmc_backend),
        hmc_ens_dir,
        input_params=(
            "Trajectories=22 trajL=1.1 mode=tepid StartTrajectory=4 "
            "lvl_sizes=8,2,2 md_name=Foo,Bar,Baz MDsteps=3"
        ),
        job_params="exec_path=/bin/Nf2p1p1 bind_script=/bind/gpu.sh",
    )
    vals = _hmc_vals(xml_path.read_text())
    assert vals["Trajectories"] == "22"
    assert vals["StartTrajectory"] == "4"
    assert vals["trajL"] == "1.1"
    assert vals["MDsteps"] == "3"
    assert vals["lvl_sizes"] == ["8", "2", "2"]
    assert vals["md_name"] == ["Foo", "Bar", "Baz"]

    script = next((hmc_ens_dir / "cnfg" / "slurm").glob("hmc_gpu_*.sh")).read_text()
    assert re.search(r"n_trajec=22", script)


def test_hmc_script_prefers_n_trajec_when_both_alias_names_given(hmc_ens_dir, hmc_backend, monkeypatch):
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(
        "MDWFutils.cli.runtime.get_backend",
        lambda _url: hmc_backend,
    )

    xml_path = _run_hmc_command(
        HMCGPUCommand(backend=hmc_backend),
        hmc_ens_dir,
        input_params="n_trajec=15 Trajectories=99 trajL=1.0 mode=tepid lvl_sizes=9,1,1",
        job_params="exec_path=/bin/Nf2p1p1 bind_script=/bind/gpu.sh",
    )
    vals = _hmc_vals(xml_path.read_text())
    assert vals["Trajectories"] == "15"

    script = next((hmc_ens_dir / "cnfg" / "slurm").glob("hmc_gpu_*.sh")).read_text()
    assert re.search(r"n_trajec=15", script)


@pytest.mark.parametrize(
    ("cmd_cls", "input_params", "expected"),
    [
        (
            HMCXMLCommand,
            "n_trajec=11 trajL=3.3 mode=continue config_start=7 lvl_sizes=5,5,5",
            {"Trajectories": "11", "StartTrajectory": "7", "trajL": "3.3"},
        ),
        (
            HMCXMLCommand,
            "Trajectories=9 trajL=0.5 mode=tepid StartTrajectory=1 lvl_sizes=9,1,1",
            {"Trajectories": "9", "StartTrajectory": "1", "trajL": "0.5"},
        ),
    ],
)
def test_hmc_xml_alias_paths(
    cmd_cls,
    input_params,
    expected,
    hmc_ens_dir,
    hmc_backend,
    monkeypatch,
):
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(
        "MDWFutils.cli.runtime.get_backend",
        lambda _url: hmc_backend,
    )

    xml_path = _run_hmc_command(
        cmd_cls(backend=hmc_backend),
        hmc_ens_dir,
        input_params=input_params,
    )
    vals = _hmc_vals(xml_path.read_text())
    for key, value in expected.items():
        assert vals[key] == value


def test_hmc_gpu_script_includes_resubmit_template(hmc_ens_dir, hmc_backend, monkeypatch):
    """HMC GPU script should include hmc_resubmit.j2 template, not source Python module."""
    monkeypatch.setenv("MDWF_DB_URL", "mongodb://fake/test")
    monkeypatch.setattr(
        "MDWFutils.cli.runtime.get_backend",
        lambda _url: hmc_backend,
    )

    xml_path = _run_hmc_command(
        HMCGPUCommand(backend=hmc_backend),
        hmc_ens_dir,
        input_params="n_trajec=15 trajL=2.6 mode=reseed config_start=0 lvl_sizes=9,1,1",
        job_params="exec_path=/bin/Nf2p1p1 bind_script=/bind/gpu.sh cfg_max=100",
    )
    assert xml_path.is_file()

    script = next((hmc_ens_dir / "cnfg" / "slurm").glob("hmc_gpu_*.sh")).read_text()

    # Template should be included inline
    assert "hmc_auto_resubmit" in script
    assert 'sbatch --dependency=afterok:$SLURM_JOBID "$batch"' in script

    # Should NOT contain the old Python source command
    assert "python -m MDWFutils.jobs.hmc_resubmit" not in script
