"""Generate help text and validate CLI parameters."""

from __future__ import annotations

from typing import Dict, List

from ..exceptions import ValidationError
from .param_schemas import ParamDef
# ContextParam has same structure as ParamDef, so we can use it interchangeably
try:
    from ..jobs.schema import ContextParam
except ImportError:
    # During migration, ContextParam might not exist yet
    ContextParam = None


class HelpGenerator:
    """Helper for producing help descriptions and validating parameters."""

    @staticmethod
    def generate_help(schema: List, heading: str = "Parameters") -> str:
        """Generate help text from schema (works with ParamDef or ContextParam)."""
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
            # Don't apply schema defaults for job params - let context builders handle defaults
            # Schema defaults are only for documentation/help text
            value = params.get(definition.name)
            
            if value is None:
                if definition.required:
                    missing_required.append(definition)
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

    @staticmethod
    def apply_defaults_and_validate(params: Dict[str, str], schema: List, param_type: str = "parameter") -> Dict:
        """Apply schema defaults and validate parameters.
        
        Unlike validate_and_cast(), this method DOES apply defaults from the schema.
        Used for context builder schemas where defaults should be applied.
        
        Args:
            params: Parameters to validate (may be missing some)
            schema: Parameter schema definitions (ParamDef or ContextParam)
            param_type: Type hint for error messages ("input" or "job")
        
        Returns:
            Dict with defaults applied and values type-cast
        """
        typed: Dict[str, object] = {}
        errors: List[str] = []
        missing_required: List = []

        for definition in schema:
            # Get value from params, or use schema default if not provided
            value = params.get(definition.name)
            
            if value is None:
                if definition.required:
                    missing_required.append(definition)
                    continue
                elif definition.default is not None:
                    # Apply schema default
                    value = definition.default
                else:
                    # No value and no default - skip this param
                    continue

            # Validate choices if specified
            if definition.choices and value not in definition.choices:
                errors.append(
                    f"{definition.name} must be one of: {', '.join(map(str, definition.choices))}"
                )
                continue

            # Type cast the value
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


