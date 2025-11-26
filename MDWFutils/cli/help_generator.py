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
    def validate_and_cast(params: Dict[str, str], schema: List[ParamDef], param_type: str = "parameter") -> Dict:
        """Validate and cast parameters.
        
        Args:
            params: Parameters to validate
            schema: Parameter schema definitions
            param_type: Type hint for error messages ("input" or "job")
        """
        typed: Dict[str, object] = {}
        errors: List[str] = []
        missing_required: List[ParamDef] = []

        for definition in schema:
            value = params.get(definition.name, definition.default)

            if definition.required and value is None:
                missing_required.append(definition)
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

        if missing_required:
            flag = "-i" if param_type == "input" else "-j"
            msg = f"\nMissing required {param_type} parameters (pass with {flag}):\n"
            for param in missing_required:
                msg += f"  • {param.name}: {param.help}\n"
            examples = " ".join(f"{p.name}=<value>" for p in missing_required)
            msg += f"\nExample: {flag} \"{examples}\""
            errors.append(msg)

        if errors:
            raise ValidationError("\n".join(errors))

        return typed


