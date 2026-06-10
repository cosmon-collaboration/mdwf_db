"""Tests for filesystem scanners."""

from pathlib import Path

from MDWFutils.scanners.gauge_obs import GaugeObsScanner
from MDWFutils.scanners.meson2pt import Meson2ptScanner
from MDWFutils.scanners.mres import MresScanner


def test_gauge_obs_scanner_finds_configs(tmp_path, gauge_fixture_path):
    t0_dir = tmp_path / "t0"
    t0_dir.mkdir()
    (t0_dir / "t0.100.out").write_text(gauge_fixture_path.read_text())
    (t0_dir / "t0.104.out").write_text("Calculated Trace 0.5\n")
    (t0_dir / "t0.badname.out").write_text("skip\n")

    results = GaugeObsScanner().scan(tmp_path)
    assert [r.config_number for r in results] == [100, 104]


def test_gauge_obs_scanner_empty_when_no_t0_dir(tmp_path):
    assert GaugeObsScanner().scan(tmp_path) == []


def _write_mres_set(data_dir: Path, cfg: int) -> None:
    for idx in (0, 1, 2):
        (data_dir / f"Mres_{idx}ckn{cfg}.bin").write_bytes(b"")
        (data_dir / f"Mres_Mid{idx}ckn{cfg}.bin").write_bytes(b"")


def test_mres_scanner_requires_full_file_set(tmp_path):
    data_dir = tmp_path / "mres" / "DATA"
    data_dir.mkdir(parents=True)
    _write_mres_set(data_dir, 100)
    (data_dir / "Mres_0ckn200.bin").write_bytes(b"")  # incomplete config 200

    results = MresScanner().scan(tmp_path)
    assert len(results) == 1
    assert results[0].config_number == 100
    assert set(results[0].metadata["files"]["PP"]) == {0, 1, 2}


def test_meson2pt_scanner_groups_by_config(tmp_path):
    data_dir = tmp_path / "meson2pt" / "DATA"
    data_dir.mkdir(parents=True)
    (data_dir / "Meson_2pt_00ckn100.bin").write_bytes(b"")
    (data_dir / "Meson_2pt_01ckn100.bin").write_bytes(b"")
    (data_dir / "Meson_2pt_00ckn200.bin").write_bytes(b"")

    results = Meson2ptScanner().scan(tmp_path)
    by_cfg = {r.config_number: r for r in results}
    assert set(by_cfg) == {100, 200}
    assert set(by_cfg[100].metadata["mesons_found"]) == {"pion", "kaon"}
