# AGENTS.md

Role: You are a coding agent working in the `MDWFutils` repository. Your job is
to understand the existing system, make narrowly scoped changes when asked, and
leave the repository in a verified, explainable state.

This file is structured to be outcome-first: use the success criteria, context,
constraints, tools, and stop rules below to decide the shortest safe path to a
correct result. The structure follows OpenAI prompt guidance for agentic coding
workflows: clear goals, explicit tool boundaries, concise user updates,
grounded claims, and lightweight verification before finalizing.

## Goal

Maintain and extend a lightweight Python package for Domain Wall Fermion
ensemble management. The package provides the `mdwf_db` CLI for:

- MongoDB-backed ensemble metadata and operation tracking.
- SLURM script and input-file generation.
- Measurement file scanning, parsing, ingestion, and export.
- Software build script and Grid HMC source generation (`mdwf_db build`).

Do not propose broad product or architecture changes until the repository shape,
data model, CLI flow, and operational side effects are understood.

## Success Criteria

Before finalizing a task:

- The requested repo behavior or documentation change is complete.
- Relevant files, commands, and side effects have been checked.
- Any database, filesystem, credential, or external-binary dependency is called
  out when it affects verification.
- The final answer states what changed, where, and what validation ran.
- If something is blocked, the blocker is named with the smallest missing piece
  of information needed to proceed.

## Operating Context

Important directories:

- `MDWFutils/cli/`: argparse CLI, command registration, command workflow,
  formatting, and export writers.
- `MDWFutils/cli/commands/`: individual `mdwf_db` subcommands.
- `MDWFutils/backends/`: backend abstraction and MongoDB implementation.
- `MDWFutils/jobs/`: context builders for SLURM scripts and input files.
- `MDWFutils/templates/`: Jinja templates for scripts and input files.
- `MDWFutils/scanners/`: filesystem scanners for measurement files.
- `MDWFutils/parsers/`: parsers for gauge observables, mres, and meson 2pt data.
- `MDWFutils/ingest/`: scan/filter/parse/upsert orchestration.
- `MDWFutils/build/`: build context builders, beta-line catalog, grid_build validation.
- `MDWFutils/schemas/`: documented database shapes and Pydantic validators.
- `config/`: ignored local `.env` credential files.
- `tests/`: pytest unit tests (build suite, CLI hooks, HMC paths).

Runtime expectations:

- Python package metadata lives in `setup.py`.
- Full runtime dependencies are in `requirements.txt`.
- The installed console script is `mdwf_db = MDWFutils.cli:main`.
- The backend is MongoDB only.
- Most real commands require `MDWF_DB_URL`.
- `mres` and `meson2pt` parsing require `MDWF_CREADER_PATH` unless the NERSC
  default `Creader` path exists.
- Build script generation uses `MDWF_SOFTWARE_ROOT` (default NERSC path) for
  install/source/script directories; generated scripts require `MDWF_DB_URL` and
  `mdwf_db` on `PATH` when executed on a login node.
- Grid HMC workflow: `build grid init` seeds `grid_build` in the ensemble DB,
  tune parameters, `build grid cc` generates `Nf2p1p1_<action>.cc`, then
  `build grid gpu|cpu` generates compile scripts. Grid source tree must exist
  under `{source_dir}/Grid/`; gmp/mpfr must exist under `{install_cpu_dir}/`.
- `hmc-xml` `lvl_sizes` should align manually with `grid_build.nlvl1` and
  Hasenbusch count.

Useful setup:

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pip install -e .
```

## Constraints

- Preserve MongoDB-only behavior unless the user explicitly asks for another
  backend.
- Keep command-line parameter strings as `KEY=VALUE` tokens; dotted keys are
  used for nested WIT input sections.
- Do not read, print, or commit ignored `config/*.env` credential contents.
- Be careful with generated files and ensemble directories. Many commands write
  outside the package tree when pointed at real ensembles.
- Avoid broad refactors unless they are necessary for the user’s request.
- Do not treat absence of committed tests as permission to skip verification.
- Check `git status --short` before and after verification because this repo
  currently contains tracked bytecode files.

## Tool Rules

- Prefer `rg` and `rg --files` for search.
- Use `apply_patch` for manual file edits.
- Parallelize independent reads when it reduces wall-clock time.
- Do not parallelize dependent steps where one result determines the next action.
- Run mutating database commands only when the user asked for that outcome.
- Before high-impact actions such as ingesting data, deleting records, updating
  operation history, or writing into real ensemble directories, confirm the
  target and side effects are understood.

## Verification

Run pytest for build-suite and related changes:

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt && python -m pip install -e .
PYTHONPYCACHEPREFIX=/tmp/mdwf_pyc pytest -q
```

Additional low-impact CLI checks:

```bash
python -c 'from MDWFutils.cli.main import main; raise SystemExit(main())' --help
python -c 'from MDWFutils.cli.main import main; raise SystemExit(main())' build --help
python -c 'from MDWFutils.cli.main import main; raise SystemExit(main())' smear-script --params
python -c 'from MDWFutils.cli.main import main; raise SystemExit(main())' mres-script --params
PYTHONPYCACHEPREFIX=/tmp/mdwf_pyc python -m compileall -q MDWFutils
git status --short
```

Avoid plain `python -m compileall -q MDWFutils` unless you intend to touch
tracked `.pyc` artifacts.

When a real database is available and the task calls for DB-backed validation,
set `MDWF_DB_URL` first and prefer read-oriented commands:

```bash
mdwf_db status
mdwf_db scan -e <ensemble>
```

If validation cannot be run, state why and describe the next best check.

## CLI Architecture

`MDWFutils/cli/main.py` dynamically imports every module in
`MDWFutils/cli/commands/` and calls `register(subparsers)` when present. New
commands should follow that convention.

Most script/input generators should subclass `BaseCommand` from
`MDWFutils/cli/command.py` and bind direct builder classes:

- `job_builder_class` for SLURM script context generation.
- `input_builder_class` for input-file generation.
- `default_variant` for saved default parameter variants.

`BaseCommand` handles:

- `-e/--ensemble` resolution by ID, path, nickname, or current directory.
- `-i/--input-params` and `-j/--job-params` parsing.
- saved defaults via `--no-defaults`, `--update`, `--dry-run`, and `--params-variant`.
- file writing and executable bit handling for generated scripts.

Custom commands such as `query`, `ingest`, `status`, `scan`, `build`, and ensemble
administration commands manage their own argparse flow.

## Job and Template Model

Job/input generation is schema-driven:

- Builders inherit `ContextBuilder` or `WitGPUContextBuilder`.
- Every discoverable builder needs a unique `type_name`.
- Parameters are declared with `ContextParam`.
- Common SLURM/WIT defaults live in `MDWFutils/jobs/schema.py`.
- Templates are rendered through `TemplateLoader` and `TemplateRenderer`.

Builder discovery in `MDWFutils/jobs/registry.py` is dynamic. A builder with a
non-empty `job_params_schema` is treated as a job builder; otherwise it is
treated as an input builder. Direct class references in commands are preferred
over legacy string lookup.

Generated files usually land under ensemble-owned directories:

- HMC: `cnfg/slurm/` and `cnfg/HMCparameters.in`
- GLU smear: `cnfg_<SMEARTYPE><SMITERS>/`
- Wilson flow: `t0/`
- WIT jobs: `mres/`, `mres_mq/`, `meson2pt/`, or `Zv/`
- Software builds: `{MDWF_SOFTWARE_ROOT}/scripts/build_*.sh` and
  `{MDWF_SOFTWARE_ROOT}/scripts/grid_scripts/`

## Database Model

The MongoDB backend manages four collections:

- `ensembles`: ensemble metadata, physics parameters, configuration ranges,
  HMC paths, `grid_build` params, nicknames, tags, and notes.
- `operations`: operation history and SLURM tracking.
- `measurements`: ingested measurement documents by ensemble/config/type.
- `ensemble_defaults`: per-param default storage by ensemble/command/variant.

Important behavior:

- Ensemble IDs are assigned by looking up the current highest `ensemble_id`.
- Directories, nicknames, and ensemble IDs have unique indexes.
- `resolve_ensemble_identifier()` accepts numeric IDs, paths, and nicknames.
- Operation updates can target operation ID or SLURM job ID plus ensemble/type.
- Measurement ingestion uses upsert semantics.

## Measurement Ingestion and Export

The ingestion pipeline is:

1. Scanner finds candidate files and config numbers.
2. Backend fetches already measured configs.
3. Ingestor skips existing configs unless `overwrite` or `clear` is requested.
4. Parser extracts structured data.
5. Backend upserts one measurement document per config/type.

Supported measurement types:

- `gauge_obs`: scans `t0/t0.<cfg>.out`; parses text for plaquette, charge, t0,
  and w0 observables.
- `mres`: scans complete sets under `mres/DATA/`; requires `Creader`.
- `meson2pt`: scans `meson2pt/DATA/`; requires `Creader`.

Exports are handled by `MDWFutils/cli/writers.py` and support `.h5`, `.hdf5`,
`.csv`, and `.json` depending on measurement type.

## Workflow

1. Start by identifying the requested outcome and affected surface area.
2. Gather only the context needed to make the change safely.
3. Prefer established command, builder, schema, parser, scanner, and template
   patterns over new abstractions.
4. Make focused edits.
5. Run the most relevant available validation.
6. Finalize with a concise summary of changes, validation, and residual risk.

## User Updates

For multi-step work, send a short update before tool use that states the request
and first step. During longer work, update at major phase changes or when a
finding changes the plan. Keep updates short, concrete, and outcome-based; avoid
narrating routine tool calls.

## Stop Rules

Stop and ask a narrow question only when missing information would materially
change the implementation or create meaningful risk.

Stop and report a blocker when:

- Required credentials, database access, ensemble files, or external binaries are
  unavailable.
- Verification would mutate production-like data without explicit user intent.
- The requested change conflicts with the repository’s current MongoDB-only
  architecture or operational constraints.

Otherwise, continue through implementation, verification, and final explanation
within the current turn whenever feasible.

## Known Gaps

- Integration tests against live MongoDB are optional (`MDWF_DB_URL`); unit tests
  use an in-memory `FakeBackend`.
- Some help and parameter paths can be validated without a database, but most
  real workflows need MongoDB and representative ensemble directories.
- Binary parser coverage depends on an external `Creader` executable.
- Actual compilation on Perlmutter is not covered by unit tests.
## Defaults Workflow

Defaults are stored in the `ensemble_defaults` collection as structured dicts
keyed by `(ensemble_id, command, variant)`. Each param declares `storable=True`
by default; ephemeral params (config ranges, computed values) are `storable=False`.

Loading: defaults load automatically on every command. `--no-defaults` opts out.
Saving: `--update` persists merged effective params back to DB (storable only).
Partial defaults: `--update` and `--dry-run` relax required-param validation.
Migrating: `mdwf_db default_params import` reads legacy YAML files.
