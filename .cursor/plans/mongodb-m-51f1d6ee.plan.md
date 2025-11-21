<!-- 51f1d6ee-407a-446f-85bd-be0108cddf65 123902a3-3f8d-4cae-9abe-6017d53e4bed -->
# Fix Remaining Test Issues

## Issues from Test Run

From the Perlmutter test output, we have these failures:

### 1. IndentationError in hmc.py (Line 196)

**File**: `MDWFutils/jobs/hmc.py` line 196

**Error**: `IndentationError: unexpected indent`

**Problem**: Line 196 has incorrect indentation - it's indented when it shouldn't be

**Fix**: Remove the extra indentation from line 196

### 2. Remove --db-file argument entirely

**Rationale**: User confirmed only one DB exists on NERSC, so --db-file is unnecessary

**Files to modify**:

- `MDWFutils/cli/main.py` - Remove db_parent parser and --db-file argument
- `MDWFutils/cli/ensemble_utils.py` - Update `get_backend_for_args()` to use env var only
- `test/test_mongodb_cli.sh` - Remove all `--db-file="$MDWF_DB_URL"` from commands (appears ~30 times)

### 3. wit-input parameter parsing issue

**File**: `MDWFutils/jobs/wit.py` line 132

**Error**: `AttributeError: 'int' object has no attribute 'items'`

**Problem**: CLI parameters come as flat keys like `{"Configurations.first": 0}` but `_build_parameters` expects nested dicts like `{"Configurations": {"first": 0}}`

**Root cause**: `update_nested_dict()` only handles already-nested dicts, doesn't convert flat dotted keys

**Fix**: Add `_unflatten_params()` helper to convert flat keys to nested structure (like old code lines 139-146 in wit_input.py)

### 4. Test pattern mismatches

**Test 4 (nickname)**: Fails to detect "Nickname" in output - check actual output format

**Test 6 (promote)**: Fails to detect "Promoted|PRODUCTION" - check actual output format

**Test 9 (glu-input)**: Pattern checks fail - template may have different format

### 5. hmc-script command doesn't accept --db-file

**Tests 11-12**: Error shows hmc-script doesn't recognize --db-file

**Root cause**: This is from issue #2 - removing --db-file will fix this

## Implementation Steps

### Step 1: Fix indentation error in hmc.py

```python
# Line 196 - remove leading spaces
seed = seed_override if seed_override is not None else random.randint(1, 10**6)
```

### Step 2: Remove --db-file from CLI

**In `MDWFutils/cli/main.py`**:

- Remove the `db_parent` parser creation (lines ~104-111)
- Remove it from `add_parser` wrapper (lines ~116-126)
- Remove DB validation logic (lines ~143-152)
- Just use `MDWF_DB_URL` from environment

**In `MDWFutils/cli/ensemble_utils.py`**:

```python
def get_backend_for_args(args):
    """Get backend from environment variable only."""
    connection = os.getenv("MDWF_DB_URL")
    if not connection:
        connection = os.getenv("MDWF_DB", "mdwf_ensembles.db")
    return get_backend(connection)
```

**In test script** - Remove all occurrences of `--db-file="$MDWF_DB_URL"`

### Step 3: Fix wit-input parameter parsing

**In `MDWFutils/jobs/wit.py`**:

Add helper function to convert flat dotted keys to nested structure (similar to old code):

```python
def _unflatten_params(flat_params: Dict) -> Dict:
    """Convert flat dotted keys to nested dict structure."""
    result = {}
    for key, value in flat_params.items():
        parts = key.split('.')
        d = result
        for part in parts[:-1]:
            if part not in d:
                d[part] = {}
            d = d[part]
        d[parts[-1]] = value
    return result
```

Then update `build_wit_context` (line 23) to unflatten before passing to `_build_parameters`:

```python
unflattened = _unflatten_params(input_params or {})
params = _build_parameters(physics, unflattened)
```

### Step 4: Update test output patterns

After other fixes, re-run tests and update patterns for:

- nickname command output
- promote-ensemble command output  
- glu-input content format

## Files to Modify

1. `MDWFutils/jobs/hmc.py` - Fix indentation (line 196)
2. `MDWFutils/cli/main.py` - Remove --db-file argument and db_parent parser
3. `MDWFutils/cli/command.py` - Update _resolve_backend to not use args.db_file
4. `MDWFutils/cli/ensemble_utils.py` - Simplify get_backend_for_args to use env vars only
5. `MDWFutils/jobs/wit.py` - Add _unflatten_params and update build_wit_context
6. `test/test_mongodb_cli.sh` - Remove all --db-file references (~28 occurrences) and fix GLU pattern checks

## Functionality Verification

All old commands are present in new code:

- add-ensemble, clear-history, default_params, glu-input, hmc-script, hmc-xml, init-db, meson2pt-script, mres-mq-script, mres-script, nickname, promote-ensemble, query, remove-ensemble, scan, smear-script, update, wflow-script, wit-input, zv-script

All job functionality preserved:

- hmc_resubmit.py used correctly in templates
- All SLURM templates include DB tracking
- All input templates working

**init-db command**: Keep it - still useful for MongoDB:

- Creates TUNING/ and ENSEMBLES/ directory structure
- Verifies DB connection works
- MongoDB schema is auto-initialized: indexes created in MongoDBBackend.**init**, collections auto-created on first insert
- No explicit schema creation needed (unlike SQLite)

## Priority Order

**CRITICAL**:

1. Fix hmc.py indentation (blocks all HMC tests)
2. Remove --db-file from CLI (blocks all SLURM script tests)

**HIGH**:

3. Fix wit-input parsing (blocks wit-input test)
4. Remove --db-file from test script

**MEDIUM**:

5. Update test output patterns (cosmetic - tests work but fail pattern match)

### To-dos

- [ ] Phase 0: Copy MDWFutils/ to MDWFutils_old_backup/
- [ ] Phase 1: Create exceptions.py with hierarchy
- [ ] Phase 1: Create schemas/ directory structure
- [ ] Phase 1: Create validators.py with Pydantic
- [ ] Phase 2: Create backends/base.py abstract interface
- [ ] Phase 2: Implement backends/mongodb.py
- [ ] Phase 2: Move SQLite to backends/sqlite.py
- [ ] Phase 2: Create backends/__init__.py factory
- [ ] Phase 3: Create templates/loader.py
- [ ] Phase 3: Create templates/context.py
- [ ] Phase 3: Create templates/renderer.py
- [ ] Phase 3: Create 7 SLURM job templates
- [ ] Phase 3: Create 3 input file templates
- [ ] Phase 4: Create cli/param_schemas.py
- [ ] Phase 4: Create cli/help_generator.py
- [ ] Phase 4: Create cli/args.py
- [ ] Phase 4: Create cli/components.py
- [ ] Phase 5: Create cli/command.py BaseCommand
- [ ] Phase 6: Refactor smear/wflow/mres commands
- [ ] Phase 6: Refactor remaining 17 commands
- [ ] Phase 7: Create migration script
- [ ] Phase 7: Update setup.py dependencies
- [ ] Create job registry mapping job_type to (template, builder)
- [ ] Add shared context utilities for common ensemble/physics extraction
- [ ] Port smear f-string SLURM body to templates/slurm/smear.j2
- [ ] Refactor jobs/smear.py to build_smear_context function
- [ ] Port wflow f-string SLURM body to templates/slurm/wflow.j2
- [ ] Refactor jobs/wflow.py to build_wflow_context function
- [ ] Port mres f-string SLURM body to templates/slurm/mres.j2
- [ ] Refactor jobs/mres.py to build_mres_context function
- [ ] Port mres_mq f-string SLURM body to templates/slurm/mres_mq.j2
- [ ] Refactor jobs/mres_mq.py to build_mres_mq_context function
- [ ] Port meson2pt f-string SLURM body to templates/slurm/meson2pt.j2
- [ ] Refactor jobs/meson2pt.py to build_meson2pt_context function
- [ ] Port zv f-string SLURM body to templates/slurm/zv.j2
- [ ] Refactor jobs/zv.py to build_zv_context function
- [ ] Port HMC GPU f-string SLURM body to templates/slurm/hmc_gpu.j2
- [ ] Port HMC CPU f-string SLURM body to templates/slurm/hmc_cpu.j2
- [ ] Refactor jobs/hmc.py to build_hmc_context functions for CPU/GPU
- [ ] Port HMC XML generation to templates/input/hmc_xml.j2
- [ ] Port GLU input generation to templates/input/glu_input.j2
- [ ] Port WIT input generation to templates/input/wit_input.j2
- [ ] Update ScriptGenerator to use job registry for routing
- [ ] Update slurm_update_trap.py to use backend abstraction
- [ ] Update CLI main.py to prefer MDWF_DB_URL env var for Mongo
- [ ] Remove or mark deprecated old generate_*_sbatch functions
- [ ] Create verification scripts to compare old vs new output
- [ ] Test all CLI commands end-to-end with test ensemble
- [ ] Fix grep -oP → portable sed (Lines 154-155)
- [ ] Fix -x, -g, -w → -i (Lines 224, 237, 250, 271, 288)
- [ ] Fix default_params save → set (Line 427)
- [ ] Fix --input-params → --input (Lines 429-430)
- [ ] Fix scan -e → scan (Line 206)
- [ ] Fix query → query --detailed (Line 162)
- [ ] Fix default params test (Lines 446-456)
- [ ] Fix HMC XML content checks (Lines 225-227)
- [ ] Simplify success detection (remove output greps)
- [ ] Fix --params → -p consistency (Line 388)
- [ ] Test with local MongoDB - fixes validated, mongomock limitations noted