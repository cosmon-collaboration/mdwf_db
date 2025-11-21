"""Generate help text and validate CLI parameters."""

from __future__ import annotations

from typing import Dict, List

from ..exceptions import ValidationError
from .param_schemas import ParamDef


class HelpGenerator:
    """Helper for producing help descriptions and validating parameters."""

    @staticmethod
    def generate_help(schema: List[ParamDef], heading: str = "Parameters") -> str:
        if not schema:
            return ""
        lines = [f"\n{heading}:"]
        for param in schema:
            parts = [f"  {param.name}: {param.help}"]
            if param.required:
                parts.append("(required)")
            if param.default is not None:
                parts.append(f"[default: {param.default}]")
            if param.choices:
                parts.append(f"[choices: {', '.join(map(str, param.choices))}]")
            lines.append(" ".join(parts))
        return "\n".join(lines)

    @staticmethod
    def validate_and_cast(params: Dict[str, str], schema: List[ParamDef]) -> Dict:
        typed: Dict[str, object] = {}
        errors: List[str] = []

        for definition in schema:
            value = params.get(definition.name, definition.default)

            if definition.required and value is None:
                errors.append(f"Missing required parameter: {definition.name}")
                continue

            if value is None:
                continue

            if definition.choices and value not in definition.choices:
                errors.append(
                    f"{definition.name} must be one of: {', '.join(map(str, definition.choices))}"
                )
                continue

            try:
                typed[definition.name] = definition.type(value)
            except (TypeError, ValueError):
                errors.append(f"{definition.name}: expected {definition.type.__name__}")

        if errors:
            raise ValidationError("\n".join(errors))

        return typed


