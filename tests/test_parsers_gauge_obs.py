"""Tests for gauge observable parser."""

import math

from MDWFutils.parsers.gauge_obs import GaugeObsParser


def test_gauge_obs_parser_extracts_fields(gauge_fixture_path):
    data = GaugeObsParser().parse(gauge_fixture_path, {})
    assert data["plaq"] == 0.5821
    assert data["Q"] == 0.123
    assert data["sqrt_t0_clov"] == 0.4012
    assert data["sqrt_t0_plaq"] == 0.4023
    assert data["w0_clov"] == 0.5014
    assert data["w0_plaq"] == 0.5025


def test_gauge_obs_parser_returns_nan_on_empty_file(tmp_path):
    empty = tmp_path / "t0.0.out"
    empty.write_text("")
    data = GaugeObsParser().parse(empty, {})
    assert math.isnan(data["plaq"])
    assert math.isnan(data["Q"])
