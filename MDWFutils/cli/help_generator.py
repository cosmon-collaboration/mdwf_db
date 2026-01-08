"""Generate help text and validate CLI parameters."""

from __future__ import annotations

from typing import Dict, List

from ..exceptions import ValidationError
from ..jobs.schema import ContextParam


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
    def validate_and_cast(params: Dict[str, str], schema: List, param_type: str = "parameter") -> Dict:
        """Validate and cast parameters.
        
        Args:
            params: Parameters to validate
            schema: Parameter schema definitions
            param_type: Type hint for error messages ("input" or "job")
        """
        typed: Dict[str, object] = {}
        errors: List[str] = []
        missing_required: List[ContextParam] = []

        for definition in schema:
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

    @staticmethod
    def format_params_detailed(input_schema: List, job_schema: List, command_name: str = "") -> str:
        """Format parameter schemas for --params output.
        
        Produces a detailed, tabular view of all -i and -j parameters.
        """
        lines = []
        
        if command_name:
            lines.append(f"Parameters for: {command_name}")
            lines.append("")
        
        if input_schema:
            lines.append("Input parameters (-i \"KEY=VALUE ...\"):")
            lines.append("-" * 70)
            lines.extend(HelpGenerator._format_param_table(input_schema))
            lines.append("")
        
        if job_schema:
            lines.append("Job parameters (-j \"KEY=VALUE ...\"):")
            lines.append("-" * 70)
            lines.extend(HelpGenerator._format_param_table(job_schema))
            lines.append("")
        
        if not input_schema and not job_schema:
            lines.append("No parameters defined for this command.")
        
        lines.append("Usage example:")
        if input_schema:
            required_input = [p for p in input_schema if p.required]
            if required_input:
                example_input = " ".join(f"{p.name}=<value>" for p in required_input[:2])
                lines.append(f'  -i "{example_input}"')
        if job_schema:
            required_job = [p for p in job_schema if p.required]
            if required_job:
                example_job = " ".join(f"{p.name}=<value>" for p in required_job[:2])
                lines.append(f'  -j "{example_job}"')
        
        return "\n".join(lines)

    @staticmethod
    def _format_param_table(schema: List) -> List[str]:
        """Format a single schema as aligned table rows."""
        if not schema:
            return []
        
        # Calculate column widths
        name_width = max(len(p.name) for p in schema)
        type_width = max(len(p.type.__name__) for p in schema)
        
        lines = []
        for param in schema:
            # Build status column (required or default)
            if param.required:
                status = "(required)"
            elif param.default is not None:
                default_str = str(param.default)
                if len(default_str) > 30:
                    default_str = default_str[:27] + "..."
                status = f"[default: {default_str}]"
            else:
                status = "(optional)"
            
            # Add choices if present
            if param.choices:
                choices_str = ", ".join(map(str, param.choices))
                if len(choices_str) > 30:
                    choices_str = choices_str[:27] + "..."
                status += f" [choices: {choices_str}]"
            
            # Format: NAME  TYPE  STATUS  - HELP
            name_col = param.name.ljust(name_width)
            type_col = param.type.__name__.ljust(type_width)
            help_text = f" - {param.help}" if param.help else ""
            lines.append(f"  {name_col}  {type_col}  {status}{help_text}")
        
        return lines


