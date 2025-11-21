<!-- c2d8173e-08b0-4068-8c9d-d346ec8bd457 154bdbdd-768e-4ce2-9b65-7f2c2ec0cf5d -->
# Enhance Test Diagnostics and Fix Remaining Mongo CLI Tests

## Goals

1. **Always print CLI output** from every `mdwf_db` invocation during the test run.
2. **Make the test file structure obvious** (where `test_run/` and generated scripts live) so you can inspect artifacts easily.
3. **Fix remaining test failures** (promote, SLURM script generators, update/clear-history) using clearer diagnostics rather than guessing.

All changes are confined to `test/test_mongodb_cli.sh`.

## 1. Clarify Test Environment and File Structure

- **Test directory**: The script uses `TEST_DIR="test_run"`, which is created under the current working directory.
- **Ensemble directories**: `add-ensemble` with `--base-dir="$TEST_DIR"` creates `TUNING/` and `ENSEMBLES/` trees under `test_run/`.
- **Generated files**:
  - Input files: `test_run/HMCparameters.xml`, `test_run/glu_smear.in`, `test_run/DWF_mres.in`, etc.
  - SLURM scripts: `test_run/hmc_gpu.sh`, `hmc_cpu.sh`, `smear.sh`, `wflow.sh`, `mres.sh`, `mres_mq.sh`, `meson2pt.sh`, `zv.sh`.
- **Plan change**: At the start of `main()` echo the full context:
```bash
echo "══════════════════════════════════════════════════════════"
echo "MongoDB CLI Test Suite"
echo "══════════════════════════════════════════════════════════"
echo "Working directory: $(pwd)"
echo "Test directory: $TEST_DIR ($(realpath "$TEST_DIR" 2>/dev/null || echo 'will be created'))"
echo "Log directory: $TEST_DIR/logs"
echo "Debug mode: ${DEBUG}"
echo "Verbose mode: ${VERBOSE}"
echo "Skip cleanup: ${SKIP_CLEANUP}"
echo "══════════════════════════════════════════════════════════"
```


This makes it explicit where all files end up, and where to look for logs.

## 2. Add Debug / Verbose / Skip-Cleanup Flags

- At the very top of the script (after variable definitions):
```bash
DEBUG=${DEBUG:-0}
VERBOSE=${VERBOSE:-0}
SKIP_CLEANUP=${SKIP_CLEANUP:-0}

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose) VERBOSE=1; shift ;;
        -d|--debug) DEBUG=1; VERBOSE=1; shift ;;
        --skip-cleanup|--keep-files) SKIP_CLEANUP=1; shift ;;
        *) shift ;;
    esac
done
```

- **Behavior**:
  - `--verbose`: show all command outputs and key file contents.
  - `--debug`: implies verbose, preserves logs, prints extra context.
  - `--skip-cleanup`: skips `phase_7_cleanup` so `test_run/` remains for inspection.

## 3. Per-Test Logging: Enhanced `run_test` and Log Directories

- Change `run_test` so each test gets its own log directory:
```bash
run_test() {
    ((TESTS_RUN++))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${BLUE}Test $TESTS_RUN: $1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    TEST_LOG_DIR="$TEST_DIR/logs/test_${TESTS_RUN}"
    mkdir -p "$TEST_LOG_DIR"
}
```


This gives a stable place to store command outputs per test.

## 4. ALWAYS Print CLI Output: `run_cmd` Wrapper

Replace ad-hoc uses of `mdwf_db ... | grep` with a `run_cmd` wrapper that always prints the full CLI output and logs it:

```bash
run_cmd() {
    local log_file="$TEST_LOG_DIR/cmd_${TESTS_RUN}.log"

    echo "━━━ Running: $* ━━━"
    # Capture both stdout and stderr, show in real-time
    local cmd_output
    cmd_output=$("$@" 2>&1 | tee /dev/stderr)
    local exit_code=$?

    echo "$cmd_output" > "$log_file"

    if [[ $exit_code -ne 0 ]]; then
        echo -e "${RED}━━━ Command failed with exit code $exit_code ━━━${NC}"
        echo "Log saved to: $log_file"
    fi
    echo ""  # readability
    return $exit_code
}
```

### Use Pattern: capture + echo + grep

For each test that currently does:

```bash
if mdwf_db query ... 2>&1 | grep -q "beta"; then
    pass ...
else
    fail ...
fi
```

Update to:

```bash
local output
output=$(run_cmd mdwf_db query ... || true)
if echo "$output" | grep -q "beta"; then
    pass "..."
else
    fail "..."
fi
```

**Key guarantees**:

- Every CLI call’s full text output is printed to the terminal.
- Output is saved under `test_run/logs/test_N/cmd_N.log` for later inspection.

## 5. Better `check_file` and `inspect_file`

### `check_file` improvements

Keep existing semantics but show context on failures:

```bash
check_file() {
    local file="$1"
    local pattern="${2:-}"
    local context="${3:-}"

    if [[ ! -f "$file" ]]; then
        fail "File not found: $file"
        [[ $VERBOSE -eq 1 || $DEBUG -eq 1 ]] && echo "Expected file at: $(pwd)/$file"
        return 1
    fi

    if [[ -n "$pattern" ]] && ! grep -q "$pattern" "$file"; then
        fail "Pattern not found in $file: $pattern"
        if [[ $VERBOSE -eq 1 || $DEBUG -eq 1 ]]; then
            echo "━━━ File contents (first 50 lines) ━━━"
            head -50 "$file"
            echo "━━━ Searched for pattern: $pattern ━━━"
            echo "━━━ File path: $(realpath "$file") ━━━"
        fi
        return 1
    fi

    pass "File verified: $file${context:+ ($context)}"
}
```

### `inspect_file` helper

For deep debugging of scripts and inputs:

```bash
inspect_file() {
    local file="$1"
    local lines="${2:-50}"

    if [[ ! -f "$file" ]]; then
        echo "File not found: $file"
        return 1
    fi

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "File: $file"
    echo "Size: $(wc -c < "$file") bytes"
    echo "Lines: $(wc -l < "$file")"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "First $lines lines:"
    head -$lines "$file"
    if [[ $(wc -l < "$file") -gt $lines ]]; then
        echo "..."
        echo "(file continues for $(($(wc -l < "$file") - $lines)) more lines)"
    fi
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}
```

Use `inspect_file` inside specific tests when `DEBUG=1`.

## 6. Fix Specific Tests Using Real Output

### 6.1 `promote-ensemble` (Test 6)

- The command currently fails but we didn't see the output. With `run_cmd`, we will.
- Plan:
  - Call `mdwf_db promote-ensemble -e test_ens --force ...` via `run_cmd`.
  - Pattern-match on actual success

### To-dos

- [ ] Add debug/verbose/skip-cleanup flags and parsing
- [ ] Enhance run_test function with log directory creation
- [ ] Enhance check_file to show file contents on failure
- [ ] Create run_cmd wrapper to capture and log all outputs
- [ ] Add inspect_file function for file content display
- [ ] Fix Test 6 (promote-ensemble) pattern matching
- [ ] Fix Tests 15-22 (SLURM scripts) with required parameters
- [ ] Debug and fix Tests 23, 25 (update operations)
- [ ] Add timeout to Test 26 (clear-history)
- [ ] Enhance test summary with log locations and rerun hints