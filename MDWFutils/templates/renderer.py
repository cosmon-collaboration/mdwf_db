"""Template renderer with optional validation."""

from __future__ import annotations

from typing import Dict, Optional, Type

from pydantic import BaseModel, ValidationError as PydanticValidationError

from ..exceptions import TemplateError, ValidationError


class TemplateRenderer:
    """Render templates with optional Pydantic validation."""

    def __init__(self, loader):
        self.loader = loader

    def render(
        self,
        template_name: str,
        context: Dict,
        schema: Optional[Type[BaseModel]] = None,
    ) -> str:
        """Render a template to a string."""
        if schema:
            try:
                schema(**context)
            except PydanticValidationError as exc:
                raise ValidationError(str(exc)) from exc

        try:
            template = self.loader.load(template_name)
            return template.render(**context)
        except Exception as exc:  # pragma: no cover - template errors
            raise TemplateError(str(exc)) from exc


