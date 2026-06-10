"""Tests for measurement ingestion orchestrator."""

from MDWFutils.ingest.orchestrator import MeasurementIngestor
from MDWFutils.parsers.gauge_obs import GaugeObsParser
from MDWFutils.scanners.gauge_obs import GaugeObsScanner


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
