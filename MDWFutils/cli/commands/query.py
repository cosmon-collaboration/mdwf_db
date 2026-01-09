#!/usr/bin/env python3
"""Export measurement data from the database."""

import argparse
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..ensemble_utils import get_backend_for_args
from ..formatting import print_table, format_float
from ..writers import (
    get_ensemble_name,
    expand_fields,
    write_data,
    prepare_gauge_obs_data,
    prepare_meson2pt_data,
    prepare_mres_data,
    GAUGE_OBS_FIELDS,
    MESON_ORDER,
    MESON_CORRELATORS,
)


# =============================================================================
# Field schema definitions (similar to ContextParam for job/input params)
# =============================================================================

@dataclass
class FieldDef:
    """Definition of an exportable field (analogous to ContextParam)."""
    name: str
    help: str
    expands_to: List[str] = field(default_factory=list)
    
    @property
    def is_group(self) -> bool:
        return len(self.expands_to) > 0


# Field schemas for each measurement type
GAUGE_OBS_FIELD_SCHEMA = [
    FieldDef("plaq", "Plaquette average"),
    FieldDef("Q", "Topological charge"),
    FieldDef("sqrt_t0_clov", "sqrt(t0) from clover definition"),
    FieldDef("sqrt_t0_plaq", "sqrt(t0) from plaquette definition"),
    FieldDef("w0_clov", "w0 scale from clover definition"),
    FieldDef("w0_plaq", "w0 scale from plaquette definition"),
]

MRES_FIELD_SCHEMA = [
    FieldDef("light", "Light quark", expands_to=["light_PP", "light_MP"]),
    FieldDef("strange", "Strange quark", expands_to=["strange_PP", "strange_MP"]),
    FieldDef("charm", "Charm quark", expands_to=["charm_PP", "charm_MP"]),
    FieldDef("light_PP", "Light quark PP correlator"),
    FieldDef("light_MP", "Light quark MP (midpoint) correlator"),
    FieldDef("strange_PP", "Strange quark PP correlator"),
    FieldDef("strange_MP", "Strange quark MP correlator"),
    FieldDef("charm_PP", "Charm quark PP correlator"),
    FieldDef("charm_MP", "Charm quark MP correlator"),
]

MESON2PT_FIELD_SCHEMA = [
    FieldDef("pion", "Pion", expands_to=["pion_PP", "pion_AP"]),
    FieldDef("kaon", "Kaon", expands_to=["kaon_PP", "kaon_AP"]),
    FieldDef("eta_s", "Eta_s", expands_to=["eta_s_PP", "eta_s_AP"]),
    FieldDef("D", "D meson", expands_to=["D_PP", "D_AP"]),
    FieldDef("Ds", "Ds meson", expands_to=["Ds_PP", "Ds_AP"]),
    FieldDef("eta_c", "Eta_c", expands_to=["eta_c_PP", "eta_c_AP"]),
    FieldDef("pion_PP", "Pion pseudoscalar-pseudoscalar correlator"),
    FieldDef("pion_AP", "Pion axial-pseudoscalar correlator"),
    FieldDef("kaon_PP", "Kaon PP correlator"),
    FieldDef("kaon_AP", "Kaon AP correlator"),
    FieldDef("eta_s_PP", "Eta_s PP correlator"),
    FieldDef("eta_s_AP", "Eta_s AP correlator"),
    FieldDef("D_PP", "D meson PP correlator"),
    FieldDef("D_AP", "D meson AP correlator"),
    FieldDef("Ds_PP", "Ds meson PP correlator"),
    FieldDef("Ds_AP", "Ds meson AP correlator"),
    FieldDef("eta_c_PP", "Eta_c PP correlator"),
    FieldDef("eta_c_AP", "Eta_c AP correlator"),
]

# Registry mapping measurement type to field schema
FIELD_SCHEMAS = {
    'gauge_obs': GAUGE_OBS_FIELD_SCHEMA,
    'mres': MRES_FIELD_SCHEMA,
    'meson2pt': MESON2PT_FIELD_SCHEMA,
}


# =============================================================================
# Help text generation from field schemas
# =============================================================================

def format_field_schema(schema: List[FieldDef], title: str = "") -> str:
    """Format field schema for --list-fields output."""
    lines = []
    
    if title:
        lines.append(title)
        lines.append("-" * 70)
    
    name_width = max(len(f.name) for f in schema)
    
    groups = [f for f in schema if f.is_group]
    if groups:
        lines.append("  Group fields (expand to multiple correlators):")
        for f in groups:
            name_col = f.name.ljust(name_width)
            expands = ", ".join(f.expands_to)
            lines.append(f"    {name_col}  {f.help} -> [{expands}]")
        lines.append("")
    
    individuals = [f for f in schema if not f.is_group]
    if individuals:
        lines.append("  Individual fields:")
        for f in individuals:
            name_col = f.name.ljust(name_width)
            lines.append(f"    {name_col}  {f.help}")
    
    return "\n".join(lines)


def generate_fields_help(measurement_type: str) -> str:
    """Generate help text for --list-fields for a measurement type."""
    schema = FIELD_SCHEMAS.get(measurement_type, [])
    if not schema:
        return f"No fields defined for {measurement_type}"
    
    title_map = {
        'gauge_obs': "Gauge Observable Fields (scalar values per config)",
        'mres': "Residual Mass Correlator Fields (arrays per config)",
        'meson2pt': "Meson 2pt Correlator Fields (arrays per config)",
    }
    title = title_map.get(measurement_type, measurement_type)
    
    return format_field_schema(schema, title)


def generate_brief_fields_list(measurement_type: str) -> str:
    """Generate brief field list for help description."""
    schema = FIELD_SCHEMAS.get(measurement_type, [])
    if not schema:
        return ""
    
    field_names = [f.name for f in schema if not f.is_group]
    groups = [f.name for f in schema if f.is_group]
    
    lines = ["AVAILABLE FIELDS:"]
    if groups:
        lines.append(f"  Groups: {', '.join(groups)}")
    lines.append(f"  Fields: {', '.join(field_names[:6])}")
    if len(field_names) > 6:
        lines.append(f"          ... and {len(field_names) - 6} more")
    lines.append("  Use --list-fields for details")
    
    return "\n".join(lines)


# =============================================================================
# Helper functions
# =============================================================================

def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common arguments for export subcommands."""
    parser.add_argument(
        '-e', '--ensemble',
        nargs='+',
        metavar='ENSEMBLE',
        help='Ensemble(s) to export (required unless using --list-fields)',
    )
    parser.add_argument(
        '-o', '--output',
        type=Path,
        metavar='FILE',
        help='Output file (.h5, .csv, or .json). Default: print to stdout',
    )
    parser.add_argument(
        '--cfg-range',
        nargs=2,
        type=int,
        metavar=('START', 'END'),
        help='Filter to config range START-END inclusive (overrides thermalization)',
    )
    parser.add_argument(
        '--fields',
        nargs='+',
        metavar='FIELD',
        help='Select specific fields to export (use --list-fields to see options)',
    )
    parser.add_argument(
        '--list-fields',
        action='store_true',
        help='Show available fields and exit',
    )
    parser.add_argument(
        '--include-pretherm',
        action='store_true',
        help='Include pre-thermalization configs (default: only configs >= configurations.thermalized)',
    )


def _resolve_ensembles(backend, ensemble_args: List[str]) -> List[Dict[str, Any]]:
    """Resolve ensemble arguments to list of (ensemble_id, ensemble_doc) tuples."""
    results = []
    for identifier in ensemble_args:
        try:
            ens_id, ens = backend.resolve_ensemble_identifier(identifier)
            results.append((ens_id, ens))
        except Exception as e:
            print(f"WARNING: Could not resolve ensemble '{identifier}': {e}", file=sys.stderr)
    return results


def _get_config_filter(
    ensemble: Dict[str, Any],
    cfg_range: Optional[tuple],
    include_pretherm: bool,
) -> tuple:
    """Determine config filter range based on thermalization and explicit range."""
    if cfg_range:
        return cfg_range[0], cfg_range[1]
    
    if not include_pretherm:
        cfg = ensemble.get('configurations', {})
        therm_cfg = cfg.get('thermalized')
        if therm_cfg is not None:
            return therm_cfg, None
        else:
            ens_id = ensemble.get('ensemble_id', '?')
            print(f"WARNING: configurations.thermalized not set for ensemble {ens_id}, including all configs",
                  file=sys.stderr)
    
    return None, None


# =============================================================================
# Base query command with template pattern
# =============================================================================

class BaseQueryCommand(ABC):
    """Base class for query subcommands using template pattern."""
    
    measurement_type: str = ""
    help: str = ""
    
    @abstractmethod
    def prepare_data(self, measurements: List, physics: Dict, fields: Optional[Set]) -> Dict:
        """Prepare measurement data for export."""
        pass
    
    @abstractmethod
    def print_stdout(self, data: Dict, fields: Optional[Set]) -> None:
        """Print data to stdout."""
        pass
    
    def register(self, parser: argparse.ArgumentParser) -> None:
        """Register arguments for this command."""
        _add_common_arguments(parser)
        brief_fields = generate_brief_fields_list(self.measurement_type)
        parser.description = f"""
Export {self.measurement_type} data.

{brief_fields}
"""
    
    def execute(self, args) -> int:
        """Execute the query command."""
        if getattr(args, 'list_fields', False):
            print(generate_fields_help(self.measurement_type))
            return 0
        
        if not args.ensemble:
            print("ERROR: -e/--ensemble is required", file=sys.stderr)
            print("Hint: Use --list-fields to see available fields without an ensemble", file=sys.stderr)
            return 1
        
        backend = get_backend_for_args(args)
        ensembles = _resolve_ensembles(backend, args.ensemble)
        
        if not ensembles:
            print("No ensembles found", file=sys.stderr)
            return 1
        
        fields = expand_fields(args.fields, self.measurement_type) if args.fields else None
        output_data = {}
        
        for ens_id, ensemble in ensembles:
            config_start, config_end = _get_config_filter(
                ensemble, args.cfg_range, args.include_pretherm
            )
            
            measurements = backend.query_measurements(
                ens_id, self.measurement_type,
                config_start=config_start,
                config_end=config_end,
            )
            
            if not measurements:
                continue
            
            ens_name = get_ensemble_name(ensemble)
            physics = ensemble.get('physics', {})
            data = self.prepare_data(measurements, physics, fields)
            output_data[ens_name] = {self.measurement_type: data}
        
        if not output_data:
            print(f"No {self.measurement_type} data found", file=sys.stderr)
            return 0
        
        if args.output:
            write_data(output_data, args.output, self.measurement_type)
            print(f"Wrote {len(output_data)} ensemble(s) to {args.output}")
        else:
            self.print_stdout(output_data, fields)
        
        return 0


# =============================================================================
# Concrete query commands
# =============================================================================

class QueryGaugeObsCommand(BaseQueryCommand):
    """Export gauge observables."""
    
    measurement_type = "gauge_obs"
    help = "Export gauge observables (plaq, Q, t0, w0)"
    
    def prepare_data(self, measurements, physics, fields):
        return prepare_gauge_obs_data(measurements, fields)
    
    def print_stdout(self, data, fields):
        if fields is None:
            fields = set(GAUGE_OBS_FIELDS)
        
        for ens_name, ens_data in data.items():
            gauge_data = ens_data.get('gauge_obs', {})
            cfgs = gauge_data.get('cfgs', [])
            
            print(f"\nGauge observables for {ens_name} ({len(cfgs)} configs)\n")
            
            headers = ['CFG']
            for f in GAUGE_OBS_FIELDS:
                if f in fields and f in gauge_data:
                    headers.append(f.upper())
            
            rows = []
            for i, cfg in enumerate(cfgs):
                row = {'CFG': cfg}
                for f in GAUGE_OBS_FIELDS:
                    if f in fields and f in gauge_data:
                        val = gauge_data[f][i] if i < len(gauge_data[f]) else None
                        row[f.upper()] = format_float(val)
                rows.append(row)
            
            print_table(headers, rows)


class QueryMresCommand(BaseQueryCommand):
    """Export mres correlators."""
    
    measurement_type = "mres"
    help = "Export mres correlators (PP, MP for light/strange/charm)"
    
    def prepare_data(self, measurements, physics, fields):
        return prepare_mres_data(measurements, physics, fields)
    
    def print_stdout(self, data, fields):
        for ens_name, ens_data in data.items():
            mres_data = ens_data.get('mres', {})
            
            for mq_key, quark_data in mres_data.items():
                cfgs = quark_data.get('cfgs', [])
                print(f"\nMres for {ens_name} ({mq_key}): {len(cfgs)} configs")
                
                if cfgs:
                    print(f"  Config range: {min(cfgs)} - {max(cfgs)}")
                    print("  Configs:", ", ".join(str(c) for c in cfgs[:20]))
                    if len(cfgs) > 20:
                        print(f"  ... and {len(cfgs) - 20} more")


class QueryMeson2ptCommand(BaseQueryCommand):
    """Export meson 2pt correlators."""
    
    measurement_type = "meson2pt"
    help = "Export meson 2pt correlators (PP, AP for pion/kaon/eta_s/D/Ds/eta_c)"
    
    def prepare_data(self, measurements, physics, fields):
        return prepare_meson2pt_data(measurements, physics, fields)
    
    def print_stdout(self, data, fields):
        for ens_name, ens_data in data.items():
            meson_data = ens_data.get('meson2pt', {})
            cfgs = meson_data.get('cfgs', [])
            
            mesons_present = []
            for m in MESON_ORDER:
                if any(f"{m}_{c}" in meson_data for c in MESON_CORRELATORS):
                    mesons_present.append(m)
            
            print(f"\nMeson 2pt for {ens_name}: {len(cfgs)} configs")
            print(f"  Mesons: {', '.join(mesons_present)}")
            
            if cfgs:
                print(f"  Config range: {min(cfgs)} - {max(cfgs)}")
                print("  Configs:", ", ".join(str(c) for c in cfgs[:20]))
                if len(cfgs) > 20:
                    print(f"  ... and {len(cfgs) - 20} more")


# =============================================================================
# Query All command
# =============================================================================

class QueryAllCommand:
    """Export all measurement types."""
    
    help = "Export all measurement types (gauge_obs, mres, meson2pt)"
    
    def register(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            '-e', '--ensemble',
            nargs='+',
            metavar='ENSEMBLE',
            help='Ensemble(s) to export (required unless using --list-fields)',
        )
        parser.add_argument(
            '-o', '--output',
            type=Path,
            metavar='FILE',
            help='Output file (.h5, .csv, or .json) - required for export',
        )
        parser.add_argument(
            '--cfg-range',
            nargs=2,
            type=int,
            metavar=('START', 'END'),
            help='Filter to config range START-END inclusive',
        )
        parser.add_argument(
            '--include-pretherm',
            action='store_true',
            help='Include pre-thermalization configs',
        )
        parser.add_argument(
            '--list-fields',
            action='store_true',
            help='Show available fields for all measurement types',
        )
        parser.description = """
Export all measurement types to a single file.

Use --list-fields to see available fields for each measurement type.

EXAMPLES:
  mdwf_db query all -e 5 -o ensemble5.h5           # Single ensemble
  mdwf_db query all -e 1 3 5 -o subset.h5          # Multiple ensembles
  mdwf_db query all --list-fields                  # Show all available fields
"""
    
    def execute(self, args) -> int:
        if getattr(args, 'list_fields', False):
            for mtype in ['gauge_obs', 'mres', 'meson2pt']:
                print(generate_fields_help(mtype))
                print()
            return 0
        
        if not args.ensemble:
            print("ERROR: -e/--ensemble is required for export", file=sys.stderr)
            return 1
        if not args.output:
            print("ERROR: -o/--output is required for 'query all'", file=sys.stderr)
            return 1
        
        backend = get_backend_for_args(args)
        ensembles = _resolve_ensembles(backend, args.ensemble)
        
        if not ensembles:
            print("No ensembles found", file=sys.stderr)
            return 1
        
        output_data = {}
        
        for ens_id, ensemble in ensembles:
            config_start, config_end = _get_config_filter(
                ensemble, args.cfg_range, args.include_pretherm
            )
            physics = ensemble.get('physics', {})
            ens_name = get_ensemble_name(ensemble)
            
            ens_data = {}
            
            measurements = backend.query_measurements(
                ens_id, 'gauge_obs',
                config_start=config_start,
                config_end=config_end,
            )
            if measurements:
                ens_data['gauge_obs'] = prepare_gauge_obs_data(measurements)
            
            measurements = backend.query_measurements(
                ens_id, 'mres',
                config_start=config_start,
                config_end=config_end,
            )
            if measurements:
                ens_data['mres'] = prepare_mres_data(measurements, physics)
            
            measurements = backend.query_measurements(
                ens_id, 'meson2pt',
                config_start=config_start,
                config_end=config_end,
            )
            if measurements:
                ens_data['meson2pt'] = prepare_meson2pt_data(measurements, physics)
            
            if ens_data:
                output_data[ens_name] = ens_data
        
        if not output_data:
            print("No data found", file=sys.stderr)
            return 0
        
        write_data(output_data, args.output, 'all')
        print(f"Wrote {len(output_data)} ensemble(s) to {args.output}")
        return 0


# =============================================================================
# Command registration
# =============================================================================

class QueryCommand:
    """Container for query subcommands."""
    
    name = "query"
    help = "Export measurement data from the database"
    
    def __init__(self):
        self.commands = {
            "gauge_obs": QueryGaugeObsCommand(),
            "mres": QueryMresCommand(),
            "meson2pt": QueryMeson2ptCommand(),
            "all": QueryAllCommand(),
        }
    
    def register(self, subparsers: argparse._SubParsersAction) -> None:
        """Register the query command and its subcommands."""
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Export measurement data from the database.

SUBCOMMANDS:
  gauge_obs   Export gauge observables (plaq, Q, t0, w0)
  mres        Export mres correlators (PP, MP)
  meson2pt    Export meson 2pt correlators (PP, AP)
  all         Export all measurement types to a single file

EXAMPLES:
  mdwf_db query gauge_obs -e 5              # Export to stdout
  mdwf_db query gauge_obs -e 5 -o data.h5   # Export to HDF5
  mdwf_db query mres -e 5 --fields light    # Light quark only
  mdwf_db query all -e 5 -o ensemble5.h5    # All data for ensemble 5
  mdwf_db query gauge_obs --list-fields     # Show available fields
""",
        )
        variants = parser.add_subparsers(dest="variant", required=True)
        
        for variant, command in self.commands.items():
            variant_parser = variants.add_parser(
                variant,
                help=command.help,
                formatter_class=argparse.RawDescriptionHelpFormatter,
            )
            command.register(variant_parser)
            variant_parser.set_defaults(func=command.execute)


def register(subparsers: argparse._SubParsersAction) -> None:
    """Register the query command."""
    QueryCommand().register(subparsers)
