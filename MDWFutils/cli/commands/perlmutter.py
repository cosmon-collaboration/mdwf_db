"""Perlmutter/NERSC read-only diagnostics and helper templates."""

from __future__ import annotations

import argparse

from ...perlmutter.capabilities import collect_capabilities
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser(
        "perlmutter",
        help="Perlmutter/NERSC diagnostics and workflow helpers",
    )
    sub = p.add_subparsers(dest="action", required=True)

    doctor = sub.add_parser("doctor", help="Detect Perlmutter/NERSC capabilities")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    doctor.set_defaults(func=do_doctor)

    tmpl = sub.add_parser("scrontab-template", help="Print a safe scrontab workflow template")
    tmpl.add_argument("kind", choices=["monitor", "ingest", "archive"])
    tmpl.add_argument("--json", action="store_true", help="Print machine-readable JSON output")
    tmpl.set_defaults(func=do_scrontab_template)


def do_doctor(args):
    payload = collect_capabilities()
    if args.json:
        print_json(payload)
    else:
        print(f"Host: {payload['hostname']}")
        print(f"On Perlmutter: {payload['on_perlmutter']}")
        print("Tools:")
        for name, path in sorted(payload["tools"].items()):
            print(f"  {name}: {path or 'MISSING'}")
        for warning in payload["warnings"]:
            print(f"WARNING: {warning}")
    return 0


def do_scrontab_template(args):
    commands = {
        "monitor": "mdwf_db monitor --dry-run --json",
        "ingest": "mdwf_db ingest all --dry-run --json",
        "archive": "mdwf_db storage archive-plan --json",
    }
    payload = {
        "ok": True,
        "kind": args.kind,
        "note": "Install manually with NERSC scrontab only after reviewing command frequency.",
        "template": f"*/30 * * * * {commands[args.kind]} >> $HOME/mdwf_{args.kind}.jsonl 2>&1",
    }
    if args.json:
        print_json(payload)
    else:
        print(payload["note"])
        print(payload["template"])
    return 0
