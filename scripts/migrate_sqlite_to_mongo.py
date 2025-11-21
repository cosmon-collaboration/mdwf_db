#!/usr/bin/env python3
"""One-time migration helper from SQLite to MongoDB."""

import argparse
import sqlite3
from datetime import datetime

from MDWFutils.backends.mongodb import MongoDBBackend

PHYSICS_FIELDS = {
    "beta": float,
    "b": float,
    "Ls": int,
    "ml": float,
    "ms": float,
    "mc": float,
    "L": int,
    "T": int,
}

CONFIG_FIELDS = {
    "cfg_first": "first",
    "cfg_last": "last",
    "cfg_increment": "increment",
    "cfg_total": "total",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Migrate mdwf_ensembles.db to MongoDB")
    parser.add_argument("--sqlite", required=True, help="Path to legacy SQLite database")
    parser.add_argument("--mongo", required=True, help="MongoDB connection string")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to MongoDB")
    return parser.parse_args()


def read_sqlite(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def build_ensemble_docs(conn):
    ensembles = []
    cur = conn.cursor()
    cur.execute("SELECT id, directory, creation_time, description, status FROM ensembles")
    rows = cur.fetchall()
    for row in rows:
        params = fetch_params(conn, row["id"])
        physics = {};
        for key, caster in PHYSICS_FIELDS.items():
            if key in params:
                try:
                    physics[key] = caster(params[key])
                except Exception:
                    physics[key] = params[key]
        configurations = {}
        for old_key, new_key in CONFIG_FIELDS.items():
            if old_key in params:
                configurations[new_key] = _maybe_int(params[old_key])
        doc = {
            "ensemble_id": row["id"],
            "directory": row["directory"],
            "status": row["status"],
            "description": row["description"],
            "created_at": datetime.fromisoformat(row["creation_time"]) if row["creation_time"] else None,
            "physics": physics,
            "configurations": configurations,
            "default_params": {},
            "tags": [],
            "notes": None,
        }
        nickname = params.get("nickname")
        if nickname:
            doc["nickname"] = nickname
        ensembles.append(doc)
    return ensembles


def fetch_params(conn, ensemble_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT name, value FROM ensemble_parameters WHERE ensemble_id=?",
        (ensemble_id,),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def build_operation_docs(conn, ensembles_by_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, ensemble_id, operation_type, status, creation_time, update_time, user FROM operations"
    )
    rows = cur.fetchall()
    docs = []
    for row in rows:
        params = fetch_operation_params(conn, row["id"])
        ensemble = ensembles_by_id.get(row["ensemble_id"]) or {}
        doc = {
            "operation_id": row["id"],
            "ensemble_id": row["ensemble_id"],
            "ensemble_directory": ensemble.get("directory"),
            "operation_type": row["operation_type"],
            "status": row["status"],
            "timing": {
                "creation_time": _maybe_datetime(row["creation_time"]),
                "start_time": None,
                "update_time": _maybe_datetime(row["update_time"]),
                "end_time": None,
                "runtime_seconds": _maybe_int(params.get("runtime")),
            },
            "slurm": {
                "job_id": params.get("slurm_job"),
                "user": row["user"],
                "host": params.get("host"),
                "batch_script": params.get("batch_script"),
                "output_log": params.get("output_log"),
                "error_log": params.get("error_log"),
                "exit_code": _maybe_int(params.get("exit_code")),
                "slurm_status": params.get("slurm_status"),
            },
            "execution": {
                "run_dir": params.get("run_dir"),
                "config_start": _maybe_int(params.get("config_start")),
                "config_end": _maybe_int(params.get("config_end")),
                "config_increment": _maybe_int(params.get("config_increment")),
            },
            "chain": {
                "parent_operation_id": _maybe_int(params.get("parent_operation_id")),
                "attempt_number": _maybe_int(params.get("attempt_number")) or 1,
                "is_chain_member": params.get("is_chain_member", "false").lower() == "true",
            },
            "params": {
                k: v
                for k, v in params.items()
                if k
                not in {
                    "runtime",
                    "slurm_job",
                    "host",
                    "batch_script",
                    "output_log",
                    "error_log",
                    "exit_code",
                    "slurm_status",
                    "run_dir",
                    "config_start",
                    "config_end",
                    "config_increment",
                    "parent_operation_id",
                    "attempt_number",
                    "is_chain_member",
                }
            },
        }
        docs.append(doc)
    return docs


def fetch_operation_params(conn, operation_id):
    cur = conn.cursor()
    cur.execute(
        "SELECT name, value FROM operation_parameters WHERE operation_id=?",
        (operation_id,),
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def _maybe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return value


def _maybe_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def migrate(sqlite_path, mongo_uri, dry_run=False):
    conn = read_sqlite(sqlite_path)
    backend = MongoDBBackend(mongo_uri)
    ensembles = build_ensemble_docs(conn)
    by_id = {doc["ensemble_id"]: doc for doc in ensembles}
    operations = build_operation_docs(conn, by_id)

    if dry_run:
        print(f"Would migrate {len(ensembles)} ensembles and {len(operations)} operations")
        return

    for doc in ensembles:
        backend.db.ensembles.replace_one({"ensemble_id": doc["ensemble_id"]}, doc, upsert=True)
    for doc in operations:
        backend.db.operations.replace_one({"operation_id": doc["operation_id"]}, doc, upsert=True)
    print(f"Migrated {len(ensembles)} ensembles and {len(operations)} operations")


def main():
    args = parse_args()
    migrate(args.sqlite, args.mongo, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
