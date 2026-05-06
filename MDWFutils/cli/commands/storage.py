"""NERSC storage classification and planning commands."""

from __future__ import annotations

import argparse
import sys

from ...perlmutter.storage import classify_path, storage_plan
from ..ensemble_utils import get_backend_for_args
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser("storage", help="NERSC storage classification and archive planning")
    sub = p.add_subparsers(dest="action", required=True)

    classify = sub.add_parser("classify", help="Classify a filesystem path")
    classify.add_argument("path")
    classify.add_argument("--json", action="store_true")
    classify.set_defaults(func=do_classify)

    plan = sub.add_parser("plan", help="Plan active/durable/archive storage")
    plan.add_argument("-e", "--ensemble", help="Ensemble identifier")
    plan.add_argument("--path", help="Path to classify instead of an ensemble")
    plan.add_argument("--json", action="store_true")
    plan.set_defaults(func=do_plan)

    archive = sub.add_parser("archive-plan", help="Plan archive/transfer options")
    archive.add_argument("-e", "--ensemble", help="Ensemble identifier")
    archive.add_argument("--path", help="Path to classify instead of an ensemble")
    archive.add_argument("--json", action="store_true")
    archive.set_defaults(func=do_plan)


def do_classify(args):
    payload = {"ok": True, "classification": classify_path(args.path)}
    _emit(args, payload)
    return 0


def do_plan(args):
    if not args.path and not args.ensemble:
        _emit(args, {"ok": False, "status": "error", "summary": "storage plan requires --path or --ensemble"})
        return 1
    ensemble = None
    if args.ensemble:
        try:
            backend = get_backend_for_args(args)
            _ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
        except Exception as exc:
            _emit(args, {"ok": False, "status": "error", "summary": str(exc)})
            return 1
    payload = {"ok": True, "plan": storage_plan(path=args.path, ensemble=ensemble)}
    _emit(args, payload)
    return 0


def _emit(args, payload):
    if args.json:
        print_json(payload)
    elif not payload.get("ok", True):
        print(f"ERROR: {payload.get('summary')}", file=sys.stderr)
    else:
        print_json(payload)
