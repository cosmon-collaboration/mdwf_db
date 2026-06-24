"""Template renderer."""

from __future__ import annotations

from typing import Dict

from ..exceptions import TemplateError


class TemplateRenderer:
    """Render templates."""

    def __init__(self, loader):
        self.loader = loader

    def render(
        self,
        template_name: str,
        context: Dict,
    ) -> str:
        """Render a template to a string."""
        try:
            template = self.loader.load(template_name)
            return template.render(**context)
        except Exception as exc:  # pragma: no cover - template errors
            raise TemplateError(str(exc)) from exc


