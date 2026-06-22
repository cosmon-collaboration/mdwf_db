"""Reusable argparse argument helpers."""

from __future__ import annotations


def add_ensemble_arg(parser, required: bool = True):
    """Add ensemble argument. Set required=False when --params flag is present."""
    parser.add_argument(
        "-e",
        "--ensemble",
        required=required,
        help="Ensemble ID, directory path, or nickname",
    )


def add_params_flag(parser):
    """Add --params flag to show detailed parameter documentation."""
    parser.add_argument(
        "--params",
        action="store_true",
        help="Show detailed -i and -j parameter documentation and exit",
    )


def add_input_params_arg(parser):
    parser.add_argument(
        "-i",
        "--input-params",
        default="",
        help='Space separated key=value pairs for input files (e.g. "SMITERS=8")',
    )


def add_job_params_arg(parser):
    parser.add_argument(
        "-j",
        "--job-params",
        default="",
        help='Space separated key=value pairs for job parameters (e.g. "time_limit=06:00:00")',
    )


def add_output_file_arg(parser):
    parser.add_argument(
        "-o",
        "--output-file",
        help="Optional output file path (defaults to ensemble directory)",
    )


def add_default_params_group(parser):
    group = parser.add_argument_group("Default parameter management")
    group.add_argument(
        "--no-defaults",
        action="store_true",
        help="Do not load saved defaults from the database",
    )
    group.add_argument(
        "--update",
        action="store_true",
        help="Save effective merged parameters back as new defaults",
    )
    group.add_argument(
        "--params-variant",
        help="Named variant when loading/saving defaults (e.g. stout8, gpu)",
    )
    group.add_argument(
        "--force",
        action="store_true",
        help="Suppress staleness warnings for job chains",
    )


def add_dry_run_flag(parser):
    """Add --dry-run flag to preview parameters without writing files."""
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print effective parameters and target files without writing",
    )
