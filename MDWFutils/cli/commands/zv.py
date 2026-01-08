"""Zv script command built on BaseCommand."""

from ..command import BaseCommand
from ...jobs.zv import ZvContextBuilder
from ...jobs.wit import WitContextBuilder


class ZvCommand(BaseCommand):
    name = "zv-script"
    aliases = ["zv"]
    help = "Generate Zv measurement script"
    job_builder_class = ZvContextBuilder
    input_builder_class = WitContextBuilder
    default_variant = "default"


def register(subparsers):
    ZvCommand().register(subparsers)
