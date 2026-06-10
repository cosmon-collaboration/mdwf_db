"""Tests for export writer helpers."""

import json

import numpy as np

from MDWFutils.cli.writers import (
    _get_int_dtype,
    expand_fields,
    get_ensemble_name,
    write_data,
)


def test_get_ensemble_name_prefers_nickname():
    assert get_ensemble_name({"nickname": "a05m364", "directory": "/x"}) == "a05m364"


def test_get_ensemble_name_from_ensembles_path():
    name = get_ensemble_name(
        {"directory": "/data/ENSEMBLES/b4.0/b1.75Ls10/mc0.85/ms0.07/ml0.02/L32/T64"}
    )
    assert "b4.0" in name


def test_expand_fields_meson2pt_shorthand():
    expanded = expand_fields(["pion"], "meson2pt")
    assert expanded == {"pion_PP", "pion_AP"}


def test_expand_fields_mres_shorthand():
    expanded = expand_fields(["light"], "mres")
    assert expanded == {"light_PP", "light_MP"}


def test_write_data_json(tmp_path):
    payload = {"ensembles": {"ens1": {"gauge_obs": {"plaq": [0.5]}}}}
    out = tmp_path / "out.json"
    write_data(payload, out, "gauge_obs")
    loaded = json.loads(out.read_text())
    assert "ensembles" in loaded


def test_get_int_dtype_selection():
    assert _get_int_dtype([1, 2, 3]) == np.uint8
    assert _get_int_dtype([-1, 0, 1]) == np.int8
    assert _get_int_dtype([]) == np.int16
