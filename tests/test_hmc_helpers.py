"""Tests for HMC bash helper emission."""

import os
import subprocess
import sys

from MDWFutils.jobs.hmc_helpers import get_hmc_helpers_inline
from MDWFutils.templates.loader import TemplateLoader


def test_hmc_helpers_define_expected_functions():
    bash = get_hmc_helpers_inline()
    for name in (
        "hmc_find_latest_config",
        "hmc_validate_config",
        "hmc_list_configs",
    ):
        assert name in bash
    assert "ckpoint_EODWF_lat." in bash
    assert "ckpoint_EODWF_rng." in bash


def test_hmc_helpers_module_main_prints_bash():
    result = subprocess.run(
        [sys.executable, "-m", "MDWFutils.jobs.hmc_helpers"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "hmc_find_latest_config" in result.stdout


def _write_hmc_resubmit_template(tmp_path):
    content = TemplateLoader().load("common/hmc_resubmit.j2").render()
    path = tmp_path / "hmc_resubmit.sh"
    path.write_text(content, encoding="utf-8")
    return path


def test_hmc_resubmit_template_can_be_sourced(tmp_path):
    template_path = _write_hmc_resubmit_template(tmp_path)

    result = subprocess.run(
        [
            "bash",
            "-c",
            f"source {template_path}; type hmc_auto_resubmit",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "hmc_auto_resubmit is a function" in result.stdout


def test_hmc_resubmit_submits_next_job_on_happy_path(tmp_path):
    template_path = _write_hmc_resubmit_template(tmp_path)
    capture = tmp_path / "sbatch.args"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch = fake_bin / "sbatch"
    sbatch.write_text(
        "#!/bin/bash\n"
        "printf '<%s>\\n' \"$@\" > \"$SBATCH_CAPTURE\"\n",
        encoding="utf-8",
    )
    sbatch.chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                [
                    "set -u",
                    f"source {template_path}",
                    "cfg_max=460",
                    "n_trajec=5",
                    "start=445",
                    "batch=/global/cfs/cdirs/example/cnfg/slurm/hmc_gpu_0_100.sh",
                    "hmc_auto_resubmit",
                ]
            ),
        ],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_CAPTURE": str(capture),
            "SLURM_JOB_ID": "54922309",
        },
        text=True,
        check=True,
    )

    assert "Expected next start: 450" in result.stdout
    assert "Next job submitted to queue with dependency on current job" in result.stdout
    assert capture.read_text(encoding="utf-8") == (
        "<--dependency=afterok:54922309>\n"
        "</global/cfs/cdirs/example/cnfg/slurm/hmc_gpu_0_100.sh>\n"
    )


def test_hmc_resubmit_does_not_submit_when_target_reached(tmp_path):
    template_path = _write_hmc_resubmit_template(tmp_path)
    capture = tmp_path / "sbatch.args"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    sbatch = fake_bin / "sbatch"
    sbatch.write_text(
        "#!/bin/bash\n"
        "printf '<%s>\\n' \"$@\" > \"$SBATCH_CAPTURE\"\n",
        encoding="utf-8",
    )
    sbatch.chmod(0o755)

    result = subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                [
                    f"source {template_path}",
                    "cfg_max=450",
                    "n_trajec=5",
                    "start=445",
                    "batch=/global/cfs/cdirs/example/cnfg/slurm/hmc_gpu_0_100.sh",
                    "hmc_auto_resubmit",
                ]
            ),
        ],
        capture_output=True,
        env={
            **os.environ,
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "SBATCH_CAPTURE": str(capture),
            "SLURM_JOB_ID": "54922309",
        },
        text=True,
        check=True,
    )

    assert "Expected next start: 450" in result.stdout
    assert "No resubmission needed" in result.stdout
    assert not capture.exists()


def test_hmc_resubmit_skips_when_cfg_max_unset_under_set_u(tmp_path):
    template_path = _write_hmc_resubmit_template(tmp_path)

    result = subprocess.run(
        [
            "bash",
            "-c",
            "\n".join(
                [
                    "set -u",
                    f"source {template_path}",
                    "hmc_auto_resubmit",
                ]
            ),
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert "cfg_max not set - no automatic resubmission" in result.stdout
