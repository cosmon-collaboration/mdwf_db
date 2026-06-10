"""Tests for mdwf_db CLI entrypoint."""

import sys

import pytest

from MDWFutils.cli.main import main


def test_main_help_exits_zero():
    old = sys.argv
    try:
        sys.argv = ["mdwf_db", "--help"]
        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 0
    finally:
        sys.argv = old


def test_main_params_without_db(monkeypatch):
    monkeypatch.delenv("MDWF_DB_URL", raising=False)
    old = sys.argv
    try:
        sys.argv = ["mdwf_db", "meson2pt-script", "--params"]
        assert main() == 0
    finally:
        sys.argv = old


def test_main_requires_db_for_status(monkeypatch):
    monkeypatch.delenv("MDWF_DB_URL", raising=False)
    old = sys.argv
    try:
        sys.argv = ["mdwf_db", "status"]
        assert main() == 1
    finally:
        sys.argv = old


def test_main_no_command():
    old = sys.argv
    try:
        sys.argv = ["mdwf_db"]
        assert main() == 1
    finally:
        sys.argv = old
