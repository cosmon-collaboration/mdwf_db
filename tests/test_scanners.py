"""Tests for filesystem scanners."""

from pathlib import Path

from MDWFutils.scanners.gauge_obs import GaugeObsScanner
from MDWFutils.scanners.meson2pt import (
    Meson2ptScanner,
    discover_variants,
    make_measurement_type,
    parse_variant_label,
)
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


def test_parse_variant_label_valid_and_invalid():
    assert parse_variant_label("meson2pt") == {}
    assert parse_variant_label("meson2pt_ml0.0004_mc0.1234") == {"ml": 0.0004, "mc": 0.1234}
    assert parse_variant_label("meson2pt_ml1_ml2") is None
    assert parse_variant_label("meson2pt_mb0.1") is None
    assert parse_variant_label("meson2pt_") is None


def test_make_measurement_type_roundtrip():
    for label in ("meson2pt", "meson2pt_ml0.0004_mc0.1234", "meson2pt_ms0.1"):
        assert make_measurement_type(parse_variant_label(label)) == label


def test_discover_variants_prefers_unitary_and_requires_data(tmp_path):
    (tmp_path / "meson2pt" / "DATA").mkdir(parents=True)
    (tmp_path / "meson2pt_ml0.0004_mc0.1234" / "DATA").mkdir(parents=True)
    (tmp_path / "meson2pt_ml0.02").mkdir()  # job workdir without DATA
    (tmp_path / "random_dir").mkdir()

    variants = discover_variants(tmp_path)
    assert [v.name for v in variants] == ["meson2pt", "meson2pt_ml0.0004_mc0.1234"]
    assert variants[0].overrides == {}
    assert variants[1].overrides == {"ml": 0.0004, "mc": 0.1234}


def test_meson2pt_scanner_variant_dir(tmp_path):
    variant = tmp_path / "meson2pt_ml0.0004_mc0.1234"
    data_dir = variant / "DATA"
    data_dir.mkdir(parents=True)
    (data_dir / "Meson_2pt_00ckn100.bin").write_bytes(b"")
    (data_dir / "Meson_2pt_02ckn100.bin").write_bytes(b"")

    results = Meson2ptScanner(variant_dir=variant).scan(tmp_path)
    assert [r.config_number for r in results] == [100]
    assert set(results[0].metadata["mesons_found"]) == {"pion", "D"}
