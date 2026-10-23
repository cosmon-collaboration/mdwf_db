"""Tests for export writer helpers."""

import json

import numpy as np
import pytest

from MDWFutils.cli.writers import (
    _get_int_dtype,
    expand_fields,
    get_ensemble_name,
    prepare_meson2pt_data,
    slot_display_name,
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


MASS_RANKS = {"ml": 0.3, "ms": 0.15, "mc": 1.3}  # ranks: ms=1, ml=2, mc=3


def test_slot_display_name_orders_by_quark_mass():
    assert slot_display_name("eta_s", MASS_RANKS) == "pion"   # pair 11 -> ranks (1,1)
    assert slot_display_name("kaon", MASS_RANKS) == "kaon"    # pair 01 -> ranks (2,1)
    assert slot_display_name("pion", MASS_RANKS) == "eta_s"   # pair 00 -> ranks (2,2)
    assert slot_display_name("Ds", MASS_RANKS) == "D"         # pair 12 -> ranks (1,3)


def _meson_measurement():
    return {
        "config_number": 100,
        "data": {
            "mesons": {
                "eta_s": {"PP": [1.0], "AP": [0.5]},
                "kaon": {"PP": [2.0], "AP": [0.6]},
                "pion": {"PP": [3.0], "AP": [0.7]},
            }
        },
        "metadata": {"masses": MASS_RANKS},
    }


def test_prepare_meson2pt_rank_names():
    ranked = prepare_meson2pt_data([_meson_measurement()], {}, None, rank_names=True)
    assert ranked["pion_PP"] == [[1.0]]   # from eta_s slot (lightest pair)
    assert ranked["kaon_PP"] == [[2.0]]
    assert ranked["eta_s_PP"] == [[3.0]]
    assert ranked["masses"] == MASS_RANKS


def test_prepare_meson2pt_slot_names_default():
    slot = prepare_meson2pt_data([_meson_measurement()], {}, None)
    assert slot["pion_PP"] == [[3.0]]
    assert slot["eta_s_PP"] == [[1.0]]
    assert "masses" not in slot


def test_write_data_hdf5_variant_group_with_mass_attrs(tmp_path):
    h5py = pytest.importorskip("h5py")
    data = {
        "cfgs": [100, 200],
        "pion_PP": [[1.0], [2.0]],
        "masses": {"ml": 0.0004, "ms": 0.1, "mc": 0.1234},
    }
    out = tmp_path / "variants.h5"
    write_data({"ens": {"meson2pt_ml0.0004_mc0.1234": data}}, out, "meson2pt")

    with h5py.File(out, "r") as f:
        grp = f["ens/meson2pt_ml0.0004_mc0.1234"]
        assert list(grp["cfgs"][:]) == [100, 200]
        assert grp.attrs["ml"] == 0.0004
        assert grp.attrs["mc"] == 0.1234
