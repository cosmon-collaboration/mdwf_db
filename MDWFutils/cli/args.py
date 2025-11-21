"""Reusable argparse argument helpers."""

from __future__ import annotations

def add_ensemble_arg(parser):
    parser.add_argument(
        "-e",
        "--ensemble",
        required=True,
        help="Ensemble ID, directory path, or nickname",
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
        "--use-default-params",
        action="store_true",
        help="Load defaults stored in the database",
    )
    group.add_argument(
        "--save-default-params",
        action="store_true",
        help="Persist current CLI parameters as defaults",
    )
    group.add_argument(
        "--params-variant",
        help="Named variant when loading/saving defaults (e.g. stout8, gpu)",
    )

