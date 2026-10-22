"""Tests for measurement ingestion orchestrator."""

from types import SimpleNamespace

from MDWFutils.cli.commands import ingest as ingest_cmd
from MDWFutils.ingest.orchestrator import MeasurementIngestor
from MDWFutils.parsers.gauge_obs import GaugeObsParser
from MDWFutils.parsers.meson2pt import Meson2ptParser
from MDWFutils.scanners.gauge_obs import GaugeObsScanner

from tests.conftest import FakeBackend


def test_ingest_dry_run_skips_existing(fake_backend, tmp_path, gauge_fixture_path):
    t0_dir = tmp_path / "t0"
    t0_dir.mkdir()
    for cfg in (100, 104):
        (t0_dir / f"t0.{cfg}.out").write_text(gauge_fixture_path.read_text())

    fake_backend.measurements[(1, 100, "gauge_obs")] = {"data": {"plaq": 0.5}, "metadata": {}}
    ingestor = MeasurementIngestor(fake_backend, GaugeObsScanner(), GaugeObsParser(), "gauge_obs")

    result = ingestor.ingest(1, tmp_path, dry_run=True)
    assert result.would_ingest == 1
    assert result.skipped == 1
    assert fake_backend.upsert_calls == []


def test_ingest_stores_new_configs(fake_backend, tmp_path, gauge_fixture_path):
    t0_dir = tmp_path / "t0"
    t0_dir.mkdir()
    (t0_dir / "t0.100.out").write_text(gauge_fixture_path.read_text())

    ingestor = MeasurementIngestor(fake_backend, GaugeObsScanner(), GaugeObsParser(), "gauge_obs")
    result = ingestor.ingest(1, tmp_path)

    assert result.ingested == 1
    assert result.errors == []
    assert len(fake_backend.upsert_calls) == 1
    assert fake_backend.upsert_calls[0]["data"]["plaq"] == 0.5821


def test_ingest_clear_deletes_before_scan(fake_backend, tmp_path, gauge_fixture_path):
    t0_dir = tmp_path / "t0"
    t0_dir.mkdir()
    (t0_dir / "t0.100.out").write_text(gauge_fixture_path.read_text())
    fake_backend.measurements[(1, 100, "gauge_obs")] = {"data": {"plaq": 0.1}, "metadata": {}}

    ingestor = MeasurementIngestor(fake_backend, GaugeObsScanner(), GaugeObsParser(), "gauge_obs")
    result = ingestor.ingest(1, tmp_path, clear=True)

    assert result.ingested == 1
    assert fake_backend.upsert_calls[-1]["data"]["plaq"] == 0.5821


def _write_meson_files(data_dir, cfg, codes):
    for code in codes:
        (data_dir / f"Meson_2pt_{code}ckn{cfg}.bin").write_bytes(b"")


def _run_ingest_meson2pt(monkeypatch, backend, eid):
    monkeypatch.setattr(ingest_cmd, "get_backend_for_args", lambda args: backend)
    monkeypatch.setattr(
        Meson2ptParser, "_parse_creader_output", lambda self, fp, gi: [1.0, 2.0]
    )
    args = SimpleNamespace(
        ensemble=str(eid),
        creader="/bin/true",
        overwrite=False,
        clear=False,
        dry_run=False,
    )
    return ingest_cmd.IngestMeson2ptCommand().execute(args)


def test_ingest_meson2pt_variants_store_distinct_types(monkeypatch, tmp_path):
    v1 = tmp_path / "meson2pt_ml0.0004_mc0.1234" / "DATA"
    v1.mkdir(parents=True)
    _write_meson_files(v1, 100, ["00", "02"])
    v2 = tmp_path / "meson2pt_ms0.1" / "DATA"
    v2.mkdir(parents=True)
    _write_meson_files(v2, 100, ["00"])

    backend = FakeBackend()
    eid = backend.add_ensemble(str(tmp_path), {"ml": 0.02, "ms": 0.1, "mc": 0.85})

    assert _run_ingest_meson2pt(monkeypatch, backend, eid) == 0

    mtypes = sorted(c["measurement_type"] for c in backend.upsert_calls)
    assert mtypes == ["meson2pt_ml0.0004_mc0.1234", "meson2pt_ms0.1"]
    variant_call = next(
        c for c in backend.upsert_calls
        if c["measurement_type"] == "meson2pt_ml0.0004_mc0.1234"
    )
    # Label masses beat ensemble physics; unlabelled ms comes from physics
    assert variant_call["metadata"]["masses"] == {"ml": 0.0004, "ms": 0.1, "mc": 0.1234}
    assert variant_call["metadata"]["variant"] == "meson2pt_ml0.0004_mc0.1234"
    assert set(variant_call["data"]["mesons"]) == {"pion", "D"}


def test_ingest_meson2pt_unresolvable_mass_skips_variant(monkeypatch, tmp_path, capsys):
    v1 = tmp_path / "meson2pt_ml0.0004_mc0.1234" / "DATA"
    v1.mkdir(parents=True)
    _write_meson_files(v1, 100, ["00"])
    v2 = tmp_path / "meson2pt_ms0.1" / "DATA"
    v2.mkdir(parents=True)
    _write_meson_files(v2, 100, ["00"])

    backend = FakeBackend()
    # physics lacks ms: variant 1 cannot resolve it, variant 2 labels it
    eid = backend.add_ensemble(str(tmp_path), {"ml": 0.02, "mc": 0.85})

    assert _run_ingest_meson2pt(monkeypatch, backend, eid) == 0

    out = capsys.readouterr().out
    assert "Skipping meson2pt_ml0.0004_mc0.1234" in out
    assert "Cannot resolve ms" in out
    mtypes = [c["measurement_type"] for c in backend.upsert_calls]
    assert mtypes == ["meson2pt_ms0.1"]


def test_ingest_meson2pt_no_variants_reports(monkeypatch, tmp_path, capsys):
    backend = FakeBackend()
    eid = backend.add_ensemble(str(tmp_path), {"ml": 0.02, "ms": 0.1, "mc": 0.85})
    monkeypatch.setattr(ingest_cmd, "get_backend_for_args", lambda args: backend)
    args = SimpleNamespace(
        ensemble=str(eid), creader="/bin/true",
        overwrite=False, clear=False, dry_run=False,
    )

    assert ingest_cmd.IngestMeson2ptCommand().execute(args) == 1
    assert "No meson2pt/DATA or meson2pt_* variant directories found" in capsys.readouterr().out
