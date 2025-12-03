"""Introspection API for CLI commands and parameters."""

from __future__ import annotations

import argparse
import importlib
import pkgutil
from typing import Any, Dict, List

from .command import BaseCommand
from .param_schemas import ParamDef
from ..jobs.schema import ContextParam


def get_command_metadata() -> Dict[str, Any]:
    """
    Return structured metadata for all registered commands.
    
    Returns:
        Dict with structure:
        {
            "command_name": {
                "help": "Command description",
                "type": "base_command|custom|multi_variant",
                "variants": {...},  # Only for multi-variant commands
                "arguments": [...],  # All CLI arguments
                "input_params": [...],  # -i parameters (BaseCommand only)
                "job_params": [...]  # -j parameters (BaseCommand only)
            }
        }
    """
    # Create a temporary argparse subparsers to extract metadata
    temp_parser = argparse.ArgumentParser()
    subs = temp_parser.add_subparsers()
    
    commands = {}
    
    # Import and register all commands
    pkg = importlib.import_module('MDWFutils.cli.commands')
    for finder, name, ispkg in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"MDWFutils.cli.commands.{name}")
        if hasattr(mod, 'register'):
            mod.register(subs)
    
    # Extract metadata from registered parsers
    for cmd_name, cmd_parser in subs.choices.items():
        cmd_metadata = {
            "help": cmd_parser.description or "",
            "arguments": _extract_arguments(cmd_parser),
        }
        
        # Check if this is a multi-variant command (has subparsers)
        subparsers_actions = [
            action for action in cmd_parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ]
        if subparsers_actions:
            cmd_metadata["type"] = "multi_variant"
            variants = {}
            for variant_name, variant_parser in subparsers_actions[0].choices.items():
                variants[variant_name] = {
                    "help": variant_parser.description or "",
                    "arguments": _extract_arguments(variant_parser),
                }
                # Try to get input/job schemas if this is a BaseCommand variant
                input_schema, job_schema = _find_schemas_for_command(cmd_name, variant_name)
                if input_schema is not None:
                    variants[variant_name]["input_params"] = _serialize_schema(input_schema)
                    variants[variant_name]["job_params"] = _serialize_schema(job_schema)
            cmd_metadata["variants"] = variants
        else:
            cmd_metadata["type"] = "custom"
        
        # Try to get input/job schemas for single-variant BaseCommands
        if cmd_metadata["type"] == "custom":
            input_schema, job_schema = _find_schemas_for_command(cmd_name)
            if input_schema is not None:
                cmd_metadata["type"] = "base_command"
                cmd_metadata["input_params"] = _serialize_schema(input_schema)
                cmd_metadata["job_params"] = _serialize_schema(job_schema)
        
        commands[cmd_name] = cmd_metadata
    
    return commands


def _extract_arguments(parser: argparse.ArgumentParser) -> List[Dict[str, Any]]:
    """Extract argument definitions from an ArgumentParser."""
    args = []
    for action in parser._actions:
        if action.dest in ('help', 'func', 'cmd', 'variant'):
            continue
        
        arg_def = {
            "name": action.dest,
            "flags": action.option_strings or [action.dest],
            "help": action.help or "",
            "required": getattr(action, 'required', False),
        }
        
        # Determine type
        if action.type:
            arg_def["type"] = action.type.__name__
        elif isinstance(action, argparse._StoreTrueAction):
            arg_def["type"] = "flag"
            arg_def["default"] = False
        elif isinstance(action, argparse._StoreFalseAction):
            arg_def["type"] = "flag"
            arg_def["default"] = True
        else:
            arg_def["type"] = "str"
        
        if action.default is not None and action.default != argparse.SUPPRESS:
            arg_def["default"] = str(action.default)
        
        if action.choices:
            arg_def["choices"] = list(action.choices)
        
        args.append(arg_def)
    
    return args


def _find_schemas_for_command(cmd_name: str, variant: str = None) -> tuple:
    """
    Find input_schema and job_schema for a BaseCommand.
    Returns (input_schema, job_schema) or (None, None) if not a BaseCommand.
    
    Schemas are now retrieved from context builders via the registry.
    """
    try:
        # Convert cmd_name to module name (hmc-script -> hmc_script)
        module_name = cmd_name.replace('-', '_')
        mod = importlib.import_module(f"MDWFutils.cli.commands.{module_name}")
        
        # First, check for multi-variant wrapper classes (like HMCCommand)
        # These have a 'name' attribute and instantiate to have a 'commands' dict
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if isinstance(attr, type):
                # Check if this class has a 'name' attribute matching cmd_name
                if hasattr(attr, 'name') and attr.name == cmd_name:
                    # Try instantiating to see if it has 'commands' (multi-variant wrapper)
                    try:
                        wrapper_instance = attr()
                        if hasattr(wrapper_instance, 'commands') and isinstance(wrapper_instance.commands, dict):
                            # This is a multi-variant wrapper
                            if variant and variant in wrapper_instance.commands:
                                variant_cmd = wrapper_instance.commands[variant]
                                # Get schemas from context builder via registry
                                if variant_cmd.job_type:
                                    from ..jobs.registry import get_job_schema
                                    job_schema, input_schema = get_job_schema(variant_cmd.job_type)
                                    return (input_schema, job_schema)
                                # Fall back to command attributes if no job_type
                                return (variant_cmd.input_schema, variant_cmd.job_schema)
                            return (None, None)
                    except Exception:
                        # Not instantiable or doesn't have commands, continue
                        pass
        
        # Look for BaseCommand subclasses (single-variant commands)
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if (isinstance(attr, type) and 
                issubclass(attr, BaseCommand) and 
                attr is not BaseCommand):
                
                cmd_instance = attr()
                
                # Single variant command
                if hasattr(cmd_instance, 'name') and cmd_instance.name == cmd_name:
                    # Get schemas from context builder via registry
                    if cmd_instance.job_type:
                        from ..jobs.registry import get_job_schema
                        job_schema, input_schema = get_job_schema(cmd_instance.job_type)
                        return (input_schema, job_schema)
                    # Fall back to command attributes if no job_type
                    return (cmd_instance.input_schema, cmd_instance.job_schema)
    except (ImportError, AttributeError, TypeError):
        pass
    
    return (None, None)


def _serialize_schema(schema) -> List[Dict[str, Any]]:
    """Convert ParamDef or ContextParam list to JSON-serializable dicts."""
    if schema is None:
        return []
    
    result = []
    for param in schema:
        param_dict = {
            "name": param.name,
            "type": param.type.__name__,
            "required": param.required,
            "help": param.help,
        }
        if param.default is not None:
            param_dict["default"] = str(param.default)
        if hasattr(param, 'choices') and param.choices:
            param_dict["choices"] = param.choices
        result.append(param_dict)
    return result

