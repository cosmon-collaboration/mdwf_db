"""Jinja2 template loader."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from ..exceptions import TemplateError


class TemplateLoader:
    """Loads templates from the package."""

    def __init__(self, template_dir: str | None = None):
        if template_dir is None:
            template_dir = str(Path(__file__).parent)
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def load(self, template_name: str):
        """Return a compiled template by name."""
        try:
            return self.env.get_template(template_name)
        except TemplateNotFound as exc:
            raise TemplateError(f"Template not found: {template_name}") from exc


