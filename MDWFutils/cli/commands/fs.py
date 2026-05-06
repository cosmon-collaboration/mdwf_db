"""Filesystem planning helpers for Perlmutter/NERSC."""

from __future__ import annotations

import argparse
import sys

from ...perlmutter.striping import apply_stripe, stripe_plan
from ..json_output import print_json


def register(subparsers):
    p = subparsers.add_parser("fs", help="Filesystem planning helpers")
    sub = p.add_subparsers(dest="action", required=True)

    plan = sub.add_parser("stripe-plan", help="Plan Lustre striping")
    _add_stripe_args(plan)
    plan.set_defaults(func=do_stripe_plan)

    apply = sub.add_parser("stripe-apply", help="Apply a guarded Lustre striping plan")
    _add_stripe_args(apply)
    apply.add_argument("--force", action="store_true", help="Required to run lfs setstripe")
    apply.set_defaults(func=do_stripe_apply)


def _add_stripe_args(parser):
    parser.add_argument("--path", required=True)
    parser.add_argument("--mode", default="default", choices=["default", "large-hdf5", "parallel-hdf5", "many-small-files"])
    parser.add_argument("--nodes", type=int)
    parser.add_argument("--json", action="store_true")


def do_stripe_plan(args):
    payload = {"ok": True, "plan": stripe_plan(args.path, mode=args.mode, nodes=args.nodes)}
    _emit(args, payload)
    return 0


def do_stripe_apply(args):
    if not args.force:
        payload = {
            "ok": False,
            "status": "approval_required",
            "summary": "stripe-apply requires --force",
            "plan": stripe_plan(args.path, mode=args.mode, nodes=args.nodes),
        }
        _emit(args, payload)
        return 1
    payload = {"ok": True, "result": apply_stripe(args.path, mode=args.mode, nodes=args.nodes)}
    _emit(args, payload)
    return 0


def _emit(args, payload):
    if args.json:
        print_json(payload)
    elif not payload.get("ok", True):
        print(f"ERROR: {payload.get('summary')}", file=sys.stderr)
    else:
        print_json(payload)
