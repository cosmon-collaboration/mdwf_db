#!/usr/bin/env python3
"""mdwf_db build command suite."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ...build.grid_catalog import seed_grid_build
from ...build.operations import SITE_ENSEMBLE_NICKNAME
from ...build.site import DEFAULT_SOFTWARE_ROOT, resolve_site_profile
from ...exceptions import MDWFError
from ...schemas.validators import GridBuildParams
from ..build_command import write_build_artifact
from ..components import BuildScriptGenerator, EnsembleResolver
from ..ensemble_utils import resolve_ensemble_from_args
from ..runtime import load_default_backend
from ...templates.loader import TemplateLoader
from ...templates.renderer import TemplateRenderer

_FMT = argparse.RawDescriptionHelpFormatter

_BUILD_DESCRIPTION = f"""
Generate Perlmutter software build shell scripts and Grid HMC sources.

WHAT THIS DOES:
• Renders Jinja templates into executable build scripts (generate-only; does not run builds)
• Logs build operations to MongoDB when generated scripts are executed on a login node
• Uses MDWF_SOFTWARE_ROOT for source, build, install, and script paths (NERSC default below)

SITE SOFTWARE (global dependency builds):
  init-site, libxml2, qmp, qdpxx, quda, wit, wit-stack, glu
  Default ensemble nickname: software (for operation tracking only)

GRID HMC (per physics ensemble):
  grid init  — seed grid_build params in the ensemble database
  grid cc    — generate Nf2p1p1_<action>.cc from grid_build + physics
  grid gpu   — generate GPU Grid HMC compile script
  grid cpu   — generate CPU Grid HMC compile script

DEFAULT OUTPUT LOCATIONS (under $MDWF_SOFTWARE_ROOT/scripts/):
  build_<package>_gpu.sh          site GPU packages (libxml2, qmp, qdpxx, quda, wit)
  build_wit_stack.sh              WIT dependency meta-script
  build_glu.sh                    GLU CPU build
  grid_scripts/Nf2p1p1_<action>.cc
  grid_scripts/build_grid_<action>_gpu.sh | _cpu.sh

ENVIRONMENT:
  MDWF_SOFTWARE_ROOT  Software tree root (default: {DEFAULT_SOFTWARE_ROOT})
  MDWF_DB_URL         Required to generate (and when running generated scripts)

EXAMPLES:
  mdwf_db build init-site
  mdwf_db build libxml2 --show-params
  mdwf_db build wit -p parallel_jobs=16
  mdwf_db build grid init -e 5
  mdwf_db build grid cc -e 5
  mdwf_db build grid gpu -e 5 --register-paths
"""

_SITE_ENSEMBLE_HELP = (
    'Ensemble for operation logging (ID, path, or nickname; default: "software" site ensemble)'
)
_PHYSICS_ENSEMBLE_HELP = "Physics ensemble (ID, directory path, nickname, or \".\" for current directory)"
_BUILD_PARAMS_HELP = "Build parameter overrides as KEY=VALUE tokens (space-separated)"
_OUTPUT_FILE_HELP = (
    "Output file path (default: under $MDWF_SOFTWARE_ROOT/scripts/ or grid_scripts/)"
)
_SHOW_PARAMS_HELP = "Show build parameter schema and defaults, then exit"


def _generate_build(args) -> int:
    if getattr(args, "show_params", False):
        from ...build.registry import get_build_schema
        from ..help_generator import HelpGenerator

        schema = get_build_schema(args.build_type) or []
        print(HelpGenerator().format_params_detailed([], schema, command_name=args.build_type))
        return 0
    try:
        backend = load_default_backend()
        build_type = args.build_type
        from ...build.schema import parse_build_params

        build_params = parse_build_params(args.params or "")
        if getattr(args, "force_physics_mismatch", False):
            build_params["force_physics_mismatch"] = True

        resolver = EnsembleResolver(backend)
        identifier = args.ensemble or "software"
        ensemble_id, ensemble = resolver.resolve(identifier)

        generator = BuildScriptGenerator(backend)
        cmd_line = " ".join(sys.argv)
        content, context = generator.generate(
            build_type,
            ensemble_id,
            build_params,
            ensemble=ensemble,
            command_line=cmd_line,
        )
        path = write_build_artifact(content, context, args.output_file)
        print(f"Generated build artifact: {path}")

        if getattr(args, "register_paths", False):
            _register_hmc_paths(backend, ensemble_id, context)
        return 0
    except MDWFError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


def _register_hmc_paths(backend, ensemble_id: int, context: dict) -> None:
    exec_path = context.get("hmc_exec_path")
    if not exec_path:
        return
    key = "hmc_paths.exec_path"
    backend.update_ensemble(ensemble_id, **{key: exec_path})
    print(f"Updated {key}={exec_path}")


def _init_site(args) -> int:
    backend = load_default_backend()
    site = resolve_site_profile()
    renderer = TemplateRenderer(TemplateLoader())
    perm_content = renderer.render(
        "build/common/perm_fix.j2",
        {"perm_group": site.perm_group},
    )
    perm_path = Path(site.perm_fix)
    perm_path.parent.mkdir(parents=True, exist_ok=True)
    perm_path.write_text(perm_content)
    print(f"Wrote permission fix script: {perm_path}")

    try:
        eid, ens = backend.resolve_ensemble_identifier(SITE_ENSEMBLE_NICKNAME)
        print(f"Site ensemble already exists: ID={eid} directory={ens.get('directory')}")
        return 0
    except Exception:
        pass

    eid = backend.add_ensemble(
        site.base,
        {
            "beta": 4.0,
            "b": 1.0,
            "Ls": 4,
            "mc": 0.1,
            "ms": 0.1,
            "ml": 0.01,
            "L": 4,
            "T": 4,
        },
        status="PRODUCTION",
        nickname=SITE_ENSEMBLE_NICKNAME,
        description="Site software build tracking ensemble",
    )
    print(f"Created site ensemble ID={eid} nickname={SITE_ENSEMBLE_NICKNAME}")
    return 0


def _grid_init(args) -> int:
    backend = load_default_backend()
    ensemble_id, ensemble = resolve_ensemble_from_args(args)
    if not ensemble:
        return 1
    existing = ensemble.get("grid_build") or {}
    if existing and not args.force:
        print("grid_build already exists; use --force to overwrite")
        return 0
    physics = ensemble.get("physics", {})
    try:
        grid_build = seed_grid_build(physics)
        GridBuildParams(**grid_build)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    backend.update_ensemble(ensemble_id, grid_build=grid_build)
    print(f"Seeded grid_build for ensemble {ensemble_id}")
    for k, v in grid_build.items():
        print(f"  {k}: {v}")
    return 0


def _add_site_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-e", "--ensemble", default="software", help=_SITE_ENSEMBLE_HELP)
    parser.add_argument("-p", "--params", default="", help=_BUILD_PARAMS_HELP)
    parser.add_argument("-o", "--output-file", help=_OUTPUT_FILE_HELP)
    parser.add_argument("--show-params", action="store_true", help=_SHOW_PARAMS_HELP)


def _add_grid_build_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-e", "--ensemble", required=True, help=_PHYSICS_ENSEMBLE_HELP)
    parser.add_argument("-p", "--params", default="", help=_BUILD_PARAMS_HELP)
    parser.add_argument("-o", "--output-file", help=_OUTPUT_FILE_HELP)


def register(subparsers):
    build = subparsers.add_parser(
        "build",
        help="Generate software build scripts and Grid HMC sources",
        description=_BUILD_DESCRIPTION,
        formatter_class=_FMT,
    )
    subs = build.add_subparsers(dest="build_cmd", required=True)

    init_site = subs.add_parser(
        "init-site",
        help="Bootstrap site ensemble and perm_fix script",
        description="""
Bootstrap the site software environment.

WHAT THIS DOES:
• Writes {MDWF_SOFTWARE_ROOT}/scripts/perm_fix_<group>.sh from template
• Creates the site ensemble in MongoDB (nickname: software) if missing
• Site ensemble is used only for operation logging on global software builds

EXAMPLES:
  mdwf_db build init-site
        """.replace("{MDWF_SOFTWARE_ROOT}", DEFAULT_SOFTWARE_ROOT),
        formatter_class=_FMT,
    )
    init_site.set_defaults(func=_init_site)

    _PACKAGE_BUILDS = [
        (
            "libxml2",
            "libxml2_gpu",
            "Generate libxml2 GPU build script",
            """
Generate a GPU build script for libxml2 (XML parser dependency).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_libxml2_gpu.sh

EXAMPLES:
  mdwf_db build libxml2
  mdwf_db build libxml2 --show-params
  mdwf_db build libxml2 -p clean_install=false parallel_jobs=16
            """,
        ),
        (
            "qmp",
            "qmp_gpu",
            "Generate QMP GPU build script",
            """
Generate a GPU build script for QMP (QCD Message Passing).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_qmp_gpu.sh

EXAMPLES:
  mdwf_db build qmp
  mdwf_db build qmp -p base=/path/to/software
            """,
        ),
        (
            "qdpxx",
            "qdpxx_gpu",
            "Generate QDP++ GPU build script",
            """
Generate a GPU build script for QDP++ (requires QMP installed first).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_qdpxx_gpu.sh

EXAMPLES:
  mdwf_db build qdpxx
  mdwf_db build qdpxx -p parallel_jobs=32
            """,
        ),
        (
            "quda",
            "quda_gpu",
            "Generate QUDA GPU build script",
            """
Generate a GPU build script for QUDA (lattice GPU library).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_quda_gpu.sh

EXAMPLES:
  mdwf_db build quda
  mdwf_db build quda -p gpu_arch=sm_80
            """,
        ),
        (
            "wit",
            "wit_gpu",
            "Generate WIT GPU build script",
            """
Generate a GPU build script for WIT (measurement code; requires QDP++ and QUDA).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_wit_gpu.sh

EXAMPLES:
  mdwf_db build wit
  mdwf_db build wit -e software -p parallel_jobs=16
            """,
        ),
        (
            "wit-stack",
            "wit_stack",
            "Generate WIT dependency stack meta-script",
            """
Generate a meta-script that runs libxml2, qmp, qdpxx, quda, and wit builds in order.

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_wit_stack.sh

NOTE: Child scripts must exist at the expected paths under scripts/ (generate each
package script first, or run from a tree where they already exist).

EXAMPLES:
  mdwf_db build wit-stack
            """,
        ),
        (
            "glu",
            "glu_cpu",
            "Generate GLU CPU build script",
            """
Generate a CPU build script for GLU (gauge link utility / smearing).

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/build_glu.sh

EXAMPLES:
  mdwf_db build glu
  mdwf_db build glu -p install_cpu_dir=/path/to/install
            """,
        ),
    ]

    for name, btype, help_text, description in _PACKAGE_BUILDS:
        p = subs.add_parser(
            name,
            help=help_text,
            description=description.replace("$MDWF_SOFTWARE_ROOT", DEFAULT_SOFTWARE_ROOT),
            formatter_class=_FMT,
        )
        _add_site_build_args(p)
        p.set_defaults(func=_generate_build, build_type=btype)

    grid = subs.add_parser(
        "grid",
        help="Grid HMC build commands",
        description="""
Grid HMC build workflow for a physics ensemble.

WORKFLOW:
  1. mdwf_db build grid init -e <ensemble>   Seed grid_build from beta-line catalog
  2. Tune grid_build in DB (hasenbusch, nlvl1, light_mass, etc.) via mdwf_db update
  3. mdwf_db build grid cc -e <ensemble>     Generate Nf2p1p1_<action>.cc
  4. mdwf_db build grid gpu|cpu -e <ensemble>   Generate compile script

PREREQUISITES:
  Grid source tree at $MDWF_SOFTWARE_ROOT/source/Grid/
  gmp and mpfr under $MDWF_SOFTWARE_ROOT/install/

EXAMPLES:
  mdwf_db build grid init -e 5
  mdwf_db build grid cc -e my_ensemble
  mdwf_db build grid gpu -e 5 --register-paths
        """.replace("$MDWF_SOFTWARE_ROOT", DEFAULT_SOFTWARE_ROOT),
        formatter_class=_FMT,
    )
    grid_sub = grid.add_subparsers(dest="grid_cmd", required=True)

    ginit = grid_sub.add_parser(
        "init",
        help="Seed grid_build params from catalog",
        description="""
Seed grid_build parameters for a physics ensemble from the beta-line catalog.

WHAT THIS DOES:
• Picks beta_line from physics.beta (closest catalog match)
• Sets light_mass, hasenbusch, nlvl1, and EOFA defaults for physics.L
• Writes grid_build to the ensemble document in MongoDB

Idempotent: skips if grid_build already exists unless --force is given.

EXAMPLES:
  mdwf_db build grid init -e 5
  mdwf_db build grid init -e . --force
        """,
        formatter_class=_FMT,
    )
    ginit.add_argument("-e", "--ensemble", required=True, help=_PHYSICS_ENSEMBLE_HELP)
    ginit.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing grid_build (default: skip if already present)",
    )
    ginit.set_defaults(func=_grid_init)

    gcc = grid_sub.add_parser(
        "cc",
        help="Generate Nf2p1p1.cc from ensemble grid_build",
        description="""
Generate ensemble-specific Nf2p1p1.cc from physics + grid_build parameters.

WHAT THIS DOES:
• Validates grid_build against physics and the beta-line catalog
• Renders a single physics block (no #ifdef ladders) into C++ source
• Writes grid_scripts/Nf2p1p1_<action>.cc under the software scripts directory

REQUIRED:
  grid_build must exist (run 'mdwf_db build grid init' first)

EXAMPLES:
  mdwf_db build grid cc -e 5
  mdwf_db build grid cc -e 5 --force-physics-mismatch
  mdwf_db build grid cc -e 5 -o /tmp/Nf2p1p1.cc
        """,
        formatter_class=_FMT,
    )
    _add_grid_build_args(gcc)
    gcc.add_argument(
        "--force-physics-mismatch",
        action="store_true",
        help="Allow physics.beta/b/Ls/ms to differ from catalog (warn only)",
    )
    gcc.set_defaults(func=_generate_build, build_type="grid_cc")

    for variant, btype in [("gpu", "grid_hmc_gpu"), ("cpu", "grid_hmc_cpu")]:
        install_note = "install_gpu" if variant == "gpu" else "install_cpu"
        pg = grid_sub.add_parser(
            variant,
            help=f"Generate Grid HMC {variant} build script",
            description=f"""
Generate a Grid HMC {variant.upper()} compile/install script for one ensemble.

WHAT THIS DOES:
• Requires Nf2p1p1_<action>.cc (run 'mdwf_db build grid cc' first)
• Rsyncs Grid sources, configures, builds HMC binary under {install_note}/
• Logs BUILD_GRID_{variant.upper()} operations when the script is executed

DEFAULT OUTPUT:
  $MDWF_SOFTWARE_ROOT/scripts/grid_scripts/build_grid_<action>_{variant}.sh

EXAMPLES:
  mdwf_db build grid {variant} -e 5
  mdwf_db build grid {variant} -e 5 --register-paths
            """.replace("$MDWF_SOFTWARE_ROOT", DEFAULT_SOFTWARE_ROOT),
            formatter_class=_FMT,
        )
        _add_grid_build_args(pg)
        pg.add_argument(
            "--register-paths",
            action="store_true",
            help="After generation, set ensemble hmc_paths.exec_path to the Grid HMC binary",
        )
        pg.add_argument("--show-params", action="store_true", help=_SHOW_PARAMS_HELP)
        pg.set_defaults(func=_generate_build, build_type=btype)
