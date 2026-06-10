# mdwf_db test suite

Unit and integration-style tests for `MDWFutils`. No production code changes required to run these.

## Setup

```bash
python -m pip install -r requirements-dev.txt
python -m pip install -e .
```

## Run

```bash
pytest                    # full suite
pytest -q                 # quiet
pytest tests/test_jobs_wit.py -k ama   # single module/filter
pytest --cov=MDWFutils    # with coverage
```

## Layout

| Module | Covers |
|--------|--------|
| `conftest.py` | `FakeBackend`, ensemble fixtures |
| `test_jobs_*` | utils, meson2pt, wit/AMA, schema, registry |
| `test_scanners.py` / `test_parsers_*` | filesystem discovery + gauge_obs parsing |
| `test_ingest.py` | `MeasurementIngestor` with fake backend |
| `test_builders.py` | SLURM/WIT generation via context builders |
| `test_cli_*` | help, command schema merge, main entrypoint |
| `test_writers.py` | export helpers |
| `fixtures/` | committed sample measurement files |

Pytest uses `tmp_path` for filesystem fixtures and `FakeBackend` for database-free CLI/builder
tests. For manual local ensemble trees, use `--base-dir local_test` with `init-db` /
`add-ensemble` — not the repository root.
