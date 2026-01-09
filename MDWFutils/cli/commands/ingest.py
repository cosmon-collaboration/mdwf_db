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
    
    help = "Ingest all available measurement types"
    
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
        """Execute the ingest all command."""
        backend = get_backend_for_args(args)
        ensemble_id, ensemble = backend.resolve_ensemble_identifier(args.ensemble)
        if ensemble_id is None:
            return 1
        
        ensemble_path = Path(ensemble['directory'])
        
        # Register all ingestors (lazily, to handle missing dependencies)
        ensemble_physics = ensemble.get('physics', {})
        
        def get_ingestors():
            yield ("gauge_obs", GaugeObsScanner(), GaugeObsParser())
            # mres and meson2pt require creader - skip if not available
            try:
                yield ("mres", MresScanner(), MresParser(ensemble_physics=ensemble_physics))
            except ValueError as e:
                print(f"Skipping mres: {e}")
            try:
                yield ("meson2pt", Meson2ptScanner(), Meson2ptParser())
            except ValueError as e:
                print(f"Skipping meson2pt: {e}")
        
        total_ingested = 0
        total_skipped = 0
        total_errors = 0
        total_would_ingest = 0
        
        for measurement_type, scanner, parser in get_ingestors():
            ingestor = MeasurementIngestor(backend, scanner, parser, measurement_type)
            result = ingestor.ingest(
                ensemble_id,
                ensemble_path,
                overwrite=args.overwrite,
                clear=args.clear,
                dry_run=args.dry_run,
            )
            
            if args.dry_run:
                print(f"{measurement_type}: would ingest {result.would_ingest}, skip {result.skipped}")
                total_would_ingest += result.would_ingest
            else:
                print(f"{measurement_type}: ingested {result.ingested}, skipped {result.skipped}")
                if result.errors:
                    error_configs = [str(cfg) for cfg, _ in result.errors]
                    print(f"  Errors on {len(result.errors)} configs: {', '.join(error_configs[:10])}")
                total_ingested += result.ingested
                total_skipped += result.skipped
                total_errors += len(result.errors)
        
        if args.dry_run:
            print(f"\nTotal: would ingest {total_would_ingest}")
        else:
            print(f"\nTotal: ingested {total_ingested}, skipped {total_skipped}, errors {total_errors}")
        
        return 0


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
            description="Ingest measurement data from files into the database",
        )
        variants = parser.add_subparsers(dest="variant", required=True)
        
        for variant, command in self.commands.items():
            variant_parser = variants.add_parser(
                variant,
                help=command.help,
            )
            command.register(variant_parser)
            variant_parser.set_defaults(func=command.execute)


def register(subparsers):
    """Register the ingest command."""
    IngestCommand().register(subparsers)
