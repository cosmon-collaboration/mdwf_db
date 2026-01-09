"""Ingest measurement data into the database."""

import argparse
from pathlib import Path

from ...ingest import IngestResult, MeasurementIngestor
from ...parsers.gauge_obs import GaugeObsParser
from ...parsers.meson2pt import Meson2ptParser
from ...parsers.mres import MresParser
from ...scanners.gauge_obs import GaugeObsScanner
from ...scanners.meson2pt import Meson2ptScanner
from ...scanners.mres import MresScanner
from ..ensemble_utils import add_ensemble_argument, get_backend_for_args


class IngestGaugeObsCommand:
    """Command to ingest gauge observables."""
    
    help = "Ingest gauge observables from t0/*.out files"
    
    def register(self, parser: argparse.ArgumentParser):
        """Register arguments for this command."""
        add_ensemble_argument(parser)
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Re-parse existing measurements',
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Delete all existing measurements before ingesting',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be ingested without writing to database',
        )
    
    def execute(self, args):
        """Execute the ingest command."""
        backend = get_backend_for_args(args)
        ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
        if ensemble_id is None:
            return 1
        
        scanner = GaugeObsScanner()
        parser = GaugeObsParser()
        ingestor = MeasurementIngestor(backend, scanner, parser, "gauge_obs")
        
        ensemble_path = Path(ensemble['directory'])
        result = ingestor.ingest(
            ensemble_id,
            ensemble_path,
            overwrite=args.overwrite,
            clear=args.clear,
            dry_run=args.dry_run,
        )
        
        if args.dry_run:
            print(f"Would ingest {result.would_ingest} configs, skip {result.skipped}")
        else:
            print(f"Ingested {result.ingested} configs, skipped {result.skipped}")
            if result.errors:
                error_configs = [str(cfg) for cfg, _ in result.errors]
                print(f"Errors on {len(result.errors)} configs: {', '.join(error_configs[:10])}")
        
        return 0


class IngestMresCommand:
    """Command to ingest unitary mres measurements."""
    
    help = "Ingest unitary mres measurements from mres/DATA/Mres_*.bin files"
    
    def register(self, parser: argparse.ArgumentParser):
        """Register arguments for this command."""
        add_ensemble_argument(parser)
        parser.add_argument(
            '--creader',
            help='Path to creader binary (or use MDWF_CREADER_PATH env var)',
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Re-parse existing measurements',
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Delete all existing measurements before ingesting',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be ingested without writing to database',
        )
    
    def execute(self, args):
        """Execute the ingest mres command."""
        backend = get_backend_for_args(args)
        ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
        if ensemble_id is None:
            return 1
        
        # Get ensemble physics for mass extraction
        ensemble_physics = ensemble.get('physics', {})
        
        scanner = MresScanner()
        parser = MresParser(
            creader_path=args.creader,
            ensemble_physics=ensemble_physics,
        )
        ingestor = MeasurementIngestor(backend, scanner, parser, "mres")
        
        ensemble_path = Path(ensemble['directory'])
        result = ingestor.ingest(
            ensemble_id,
            ensemble_path,
            overwrite=args.overwrite,
            clear=args.clear,
            dry_run=args.dry_run,
        )
        
        if args.dry_run:
            print(f"Would ingest {result.would_ingest} configs, skip {result.skipped}")
        else:
            print(f"Ingested {result.ingested} configs, skipped {result.skipped}")
            if result.errors:
                error_configs = [str(cfg) for cfg, _ in result.errors]
                print(f"Errors on {len(result.errors)} configs: {', '.join(error_configs[:10])}")
        
        return 0


class IngestMeson2ptCommand:
    """Command to ingest unitary meson 2pt measurements."""
    
    help = "Ingest meson 2pt correlators from meson2pt/DATA/Meson_2pt_*.bin files"
    
    def register(self, parser: argparse.ArgumentParser):
        """Register arguments for this command."""
        add_ensemble_argument(parser)
        parser.add_argument(
            '--creader',
            help='Path to creader binary (or use MDWF_CREADER_PATH env var)',
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Re-parse existing measurements',
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Delete all existing measurements before ingesting',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be ingested without writing to database',
        )
    
    def execute(self, args):
        """Execute the ingest meson2pt command."""
        backend = get_backend_for_args(args)
        ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
        if ensemble_id is None:
            return 1
        
        scanner = Meson2ptScanner()
        parser = Meson2ptParser(creader_path=args.creader)
        ingestor = MeasurementIngestor(backend, scanner, parser, "meson2pt")
        
        ensemble_path = Path(ensemble['directory'])
        result = ingestor.ingest(
            ensemble_id,
            ensemble_path,
            overwrite=args.overwrite,
            clear=args.clear,
            dry_run=args.dry_run,
        )
        
        if args.dry_run:
            print(f"Would ingest {result.would_ingest} configs, skip {result.skipped}")
        else:
            print(f"Ingested {result.ingested} configs, skipped {result.skipped}")
            if result.errors:
                error_configs = [str(cfg) for cfg, _ in result.errors]
                print(f"Errors on {len(result.errors)} configs: {', '.join(error_configs[:10])}")
        
        return 0


class IngestAllCommand:
    """Command to ingest all available measurement types."""
    
    help = "Ingest all available measurement types (for one or all ensembles)"
    
    def register(self, parser: argparse.ArgumentParser):
        """Register arguments for this command."""
        add_ensemble_argument(parser, required=False, 
                              help_text='Ensemble to ingest (omit to ingest all ensembles)')
        parser.add_argument(
            '--creader',
            help='Path to creader binary (or set MDWF_CREADER_PATH env var)',
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Re-parse existing measurements',
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Delete all existing measurements before ingesting',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be ingested without writing to database',
        )
    
    def execute(self, args):
        """Execute the ingest all command."""
        backend = get_backend_for_args(args)
        
        # Get list of ensembles to process
        if args.ensemble:
            ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
            if ensemble_id is None:
                return 1
            ensembles = [(ensemble_id, ensemble)]
        else:
            # Process all ensembles
            all_ensembles = backend.list_ensembles(detailed=True)
            if not all_ensembles:
                print("No ensembles found")
                return 0
            ensembles = [(e.get('ensemble_id') or e.get('id'), e) for e in all_ensembles]
        
        grand_total_ingested = 0
        grand_total_skipped = 0
        grand_total_errors = 0
        grand_total_would_ingest = 0
        
        for ensemble_id, ensemble in ensembles:
            nick = ensemble.get('nickname')
            print(f"\n{'='*60}")
            print(f"Ensemble {ensemble_id}" + (f" ({nick})" if nick else ""))
            print(f"{'='*60}")
            
            ensemble_path = Path(ensemble['directory'])
            ensemble_physics = ensemble.get('physics', {})
            
            total_ingested = 0
            total_skipped = 0
            total_errors = 0
            total_would_ingest = 0
            
            for measurement_type, scanner, parser in self._get_ingestors(ensemble_physics, getattr(args, 'creader', None)):
                ingestor = MeasurementIngestor(backend, scanner, parser, measurement_type)
                result = ingestor.ingest(
                    ensemble_id,
                    ensemble_path,
                    overwrite=args.overwrite,
                    clear=args.clear,
                    dry_run=args.dry_run,
                )
                
                if args.dry_run:
                    print(f"  {measurement_type}: would ingest {result.would_ingest}, skip {result.skipped}")
                    total_would_ingest += result.would_ingest
                else:
                    print(f"  {measurement_type}: ingested {result.ingested}, skipped {result.skipped}")
                    if result.errors:
                        error_configs = [str(cfg) for cfg, _ in result.errors]
                        print(f"    Errors on {len(result.errors)} configs: {', '.join(error_configs[:10])}")
                    total_ingested += result.ingested
                    total_skipped += result.skipped
                    total_errors += len(result.errors)
            
            grand_total_ingested += total_ingested
            grand_total_skipped += total_skipped
            grand_total_errors += total_errors
            grand_total_would_ingest += total_would_ingest
        
        print(f"\n{'='*60}")
        if args.dry_run:
            print(f"Grand total: would ingest {grand_total_would_ingest}")
        else:
            print(f"Grand total: ingested {grand_total_ingested}, skipped {grand_total_skipped}, errors {grand_total_errors}")
        
        return 0
    
    def _get_ingestors(self, ensemble_physics, creader_path=None):
        """Generate ingestors, skipping those with missing dependencies."""
        yield ("gauge_obs", GaugeObsScanner(), GaugeObsParser())
        # mres and meson2pt require creader - skip if not available
        try:
            yield ("mres", MresScanner(), MresParser(creader_path=creader_path, ensemble_physics=ensemble_physics))
        except ValueError as e:
            print(f"  Skipping mres: {e}")
        try:
            yield ("meson2pt", Meson2ptScanner(), Meson2ptParser(creader_path=creader_path))
        except ValueError as e:
            print(f"  Skipping meson2pt: {e}")


class IngestCommand:
    """Container for ingest subcommands."""
    
    name = "ingest"
    help = "Ingest measurement data into the database"
    
    def __init__(self):
        self.commands = {
            "gauge_obs": IngestGaugeObsCommand(),
            "mres": IngestMresCommand(),
            "meson2pt": IngestMeson2ptCommand(),
            "all": IngestAllCommand(),
        }
    
    def register(self, subparsers):
        """Register the ingest command and its subcommands."""
        parser = subparsers.add_parser(
            self.name,
            help=self.help,
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="""Ingest measurement data from files into the database.

EXAMPLES:
  mdwf_db ingest gauge_obs -e 5          # Ingest gauge_obs for ensemble 5
  mdwf_db ingest mres -e 5               # Ingest mres for ensemble 5
  mdwf_db ingest all -e 5                # Ingest all types for ensemble 5
  mdwf_db ingest all                     # Ingest all types for ALL ensembles
  mdwf_db ingest all --dry-run           # Preview what would be ingested
  mdwf_db ingest all --overwrite         # Re-parse existing measurements
  mdwf_db ingest all --clear             # Delete existing before ingesting
""",
        )
        # Add dummy -e argument at parent level so argparse doesn't confuse it with subcommand
        parser.add_argument('-e', '--ensemble', nargs='*', help=argparse.SUPPRESS)
        parser.set_defaults(func=self._no_subcommand)
        variants = parser.add_subparsers(dest="variant")
        
        for variant, command in self.commands.items():
            variant_parser = variants.add_parser(
                variant,
                help=command.help,
            )
            command.register(variant_parser)
            variant_parser.set_defaults(func=command.execute)
    
    def _no_subcommand(self, args) -> int:
        """Called when no subcommand is specified."""
        print("ERROR: ingest requires a subcommand: gauge_obs, mres, meson2pt, or all")
        print()
        print("Examples:")
        print("  mdwf_db ingest gauge_obs -e 5")
        print("  mdwf_db ingest mres -e 5")
        print("  mdwf_db ingest all -e 5")
        print()
        print("Run 'mdwf_db ingest --help' for more information.")
        return 1


def register(subparsers):
    """Register the ingest command."""
    IngestCommand().register(subparsers)
