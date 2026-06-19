"""Tests for mdwf_db update operation_id emission."""

import argparse

from MDWFutils.cli.commands import update as update_cmd


def test_create_prints_operation_id(mock_db, site_ensemble_id, capsys):
    args = argparse.Namespace(
        ensemble=str(site_ensemble_id),
        operation_type="BUILD_LIBXML2",
        status="RUNNING",
        operation_id=None,
        params="host=test",
        user="tester",
    )
    rc = update_cmd.do_update(args)
    assert rc == 0
    out = capsys.readouterr().out
    assert "operation_id=1" in out


def test_update_by_id(mock_db, site_ensemble_id, capsys):
    oid = mock_db.add_operation(site_ensemble_id, "BUILD_LIBXML2", "RUNNING", "tester")
    args = argparse.Namespace(
        ensemble=str(site_ensemble_id),
        operation_type="BUILD_LIBXML2",
        status="COMPLETED",
        operation_id=oid,
        params="exit_code=0 runtime=10",
        user="tester",
    )
    rc = update_cmd.do_update(args)
    assert rc == 0
    op = mock_db.get_operation(site_ensemble_id, oid)
    assert op["status"] == "COMPLETED"
    assert len(mock_db.list_operations(site_ensemble_id)) == 1
