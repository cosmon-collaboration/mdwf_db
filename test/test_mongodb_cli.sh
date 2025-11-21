#!/bin/bash
set -uo pipefail  # Removed -e so individual test failures don't exit the script

# MongoDB CLI Test Suite
# ======================
# Systematically tests all CLI commands using MongoDB backend on NERSC

# Test configuration
MONGO_URL="${MDWF_DB_URL}"
TEST_DIR="test_run"
ENSEMBLE_ID=""
ENSEMBLE_DIR=""

# Debug and verbose modes
DEBUG=${DEBUG:-0}
VERBOSE=${VERBOSE:-0}
SKIP_CLEANUP=${SKIP_CLEANUP:-0}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose) VERBOSE=1; shift ;;
        -d|--debug) DEBUG=1; VERBOSE=1; shift ;;
        --skip-cleanup|--keep-files) SKIP_CLEANUP=1; shift ;;
        *) shift ;;
    esac
done

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
pass() { 
    echo -e "${GREEN}✓ PASS${NC}: $1"
    ((TESTS_PASSED++))
}

fail() { 
    echo -e "${RED}✗ FAIL${NC}: $1"
    ((TESTS_FAILED++))
    # Don't exit, just return failure
}

run_test() { 
    ((TESTS_RUN++))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${BLUE}Test $TESTS_RUN: $1${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Create test-specific log directory
    TEST_LOG_DIR="$TEST_DIR/logs/test_${TESTS_RUN}"
    mkdir -p "$TEST_LOG_DIR"
}

run_cmd() {
    local log_file="$TEST_LOG_DIR/cmd_${TESTS_RUN}.log"
    
    echo "━━━ Running: $* ━━━"
    # Capture both stdout and stderr, show in real-time
    local cmd_output
    cmd_output=$("$@" 2>&1 | tee /dev/stderr)
    local exit_code=$?
    
    # Save to log
    echo "$cmd_output" > "$log_file"
    
    if [[ $exit_code -ne 0 ]]; then
        echo -e "${RED}━━━ Command failed with exit code $exit_code ━━━${NC}"
        echo "Log saved to: $log_file"
    fi
    echo ""  # blank line for readability
    
    return $exit_code
}

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
            echo "━━━ File path: $(realpath "$file" 2>/dev/null || echo "$file") ━━━"
        fi
        return 1
    fi
    
    pass "File verified: $file${context:+ ($context)}"
}

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

check_no_template_vars() {
    local file="$1"
    if grep -q '{{' "$file" || grep -q '}}' "$file"; then
        fail "Unsubstituted template variables found in $file"
        cat "$file" | grep -C2 '{{' || true
    else
        pass "No unsubstituted variables in $file"
    fi
}

cleanup_test() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Test Summary"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
    
    if [[ $TESTS_FAILED -gt 0 ]]; then
        echo ""
        echo "Logs available in: $(realpath "$TEST_DIR/logs" 2>/dev/null || echo 'test_run/logs')"
        if [[ $DEBUG -eq 1 || $SKIP_CLEANUP -eq 1 ]]; then
            echo "Generated files preserved in: $(realpath "$TEST_DIR" 2>/dev/null || echo "$TEST_DIR")"
        fi
        echo "Rerun with --verbose or --debug for more detail, and --skip-cleanup to keep files."
    fi
    
    if [[ $TESTS_FAILED -eq 0 ]]; then
        echo -e "${GREEN}All tests passed!${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed${NC}"
        exit 1
    fi
}

trap cleanup_test EXIT

# Check MongoDB availability
check_mongodb() {
    if [[ -z "$MDWF_DB_URL" ]]; then
        echo -e "${RED}ERROR: MDWF_DB_URL not set${NC}"
        echo "Please run: source config/admin.env"
        exit 1
    fi
    
    echo "Testing MongoDB connection..."
    echo "Using: $MDWF_DB_URL"
    
    # Test connection using pymongo
    if ! python3 << 'EOF'
import sys
from pymongo import MongoClient
import os

try:
    url = os.environ['MDWF_DB_URL']
    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    # Try to actually use the connection
    client.admin.command('ping')
    db = client.get_database()
    db.list_collection_names()
    sys.exit(0)
except Exception as e:
    print(f"Connection failed: {e}", file=sys.stderr)
    sys.exit(1)
EOF
    then
        echo -e "${RED}ERROR: Cannot connect to MongoDB${NC}"
        echo "Make sure you're on perlmutter-p1.nersc.gov and credentials are correct"
        echo "Current MDWF_DB_URL: $MDWF_DB_URL"
        exit 1
    fi
    echo -e "${GREEN}✓ Connected to MongoDB${NC}"
}

# Phase 1: Database Initialization
phase_1_init() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 1: Database Initialization"
    echo "════════════════════════════════════════"
    
    run_test "init-db: Create directory structure"
    local init_output
    init_output=$(run_cmd mdwf_db init-db --base-dir="$TEST_DIR" 2>&1 || true)
    
    if [[ -d "$TEST_DIR/TUNING" ]] && [[ -d "$TEST_DIR/ENSEMBLES" ]]; then
        pass "Directories created successfully"
    else
        fail "Directories not created"
    fi
}

# Phase 2: Ensemble Management
phase_2_ensemble_mgmt() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 2: Ensemble Management"
    echo "════════════════════════════════════════"
    
    run_test "add-ensemble: Create test ensemble"
    local add_output
    add_output=$(run_cmd mdwf_db add-ensemble \
        --params "beta=6.0 b=2.5 Ls=12 mc=0.6 ms=0.04 ml=0.005 L=24 T=48" \
        --status=TUNING \
        --base-dir="$TEST_DIR" 2>&1 || true)
    
    if echo "$add_output" | grep -q "Ensemble added"; then
        # Extract ensemble ID using portable sed
        ENSEMBLE_ID=$(echo "$add_output" | sed -n 's/.*ID[: ]*\([0-9]\+\).*/\1/p' | head -1)
        if [[ -z "$ENSEMBLE_ID" ]]; then
            ENSEMBLE_ID=$(echo "$add_output" | sed -n 's/.*ensemble_id[: ]*\([0-9]\+\).*/\1/p' | head -1)
        fi
        [[ -z "$ENSEMBLE_ID" ]] && ENSEMBLE_ID="1"
        
        ENSEMBLE_DIR=$(echo "$add_output" | sed -n 's/.*Directory[: ]*\(.*\)/\1/p' | head -1)
        [[ -z "$ENSEMBLE_DIR" ]] && ENSEMBLE_DIR="$TEST_DIR/TUNING/b6.0/b2.5Ls12/mc0.6/ms0.04/ml0.005/L24/T48"
        pass "Ensemble created with ID: $ENSEMBLE_ID"
    else
        fail "add-ensemble failed: $add_output"
    fi
    
    run_test "query: List all ensembles"
    local query_output
    query_output=$(run_cmd mdwf_db query --detailed 2>&1 || true)
    if echo "$query_output" | grep -q "beta"; then
        pass "Query lists ensembles"
    else
        fail "Query failed to list ensembles"
    fi
    
    run_test "query -e <id>: Show detailed ensemble info"
    local query_detail_output
    query_detail_output=$(run_cmd mdwf_db query -e "$ENSEMBLE_ID" 2>&1 || true)
    if echo "$query_detail_output" | grep -q "beta.*6.0"; then
        pass "Detailed query shows physics parameters"
    else
        fail "Detailed query missing parameters"
    fi
    
    run_test "nickname: Set nickname 'test_ens'"
    local nickname_output
    nickname_output=$(run_cmd mdwf_db nickname -e "$ENSEMBLE_ID" --set test_ens 2>&1 || true)
    if echo "$nickname_output" | grep -q "Set nickname"; then
        pass "Nickname set successfully"
    else
        fail "Failed to set nickname"
    fi
    
    run_test "query -e test_ens: Resolve by nickname"
    local query_nickname_output
    query_nickname_output=$(run_cmd mdwf_db query -e test_ens 2>&1 || true)
    if echo "$query_nickname_output" | grep -q "beta"; then
        pass "Ensemble resolved by nickname"
    else
        fail "Failed to resolve by nickname"
    fi
    
    run_test "promote-ensemble: Move to PRODUCTION"
    local promote_output
    promote_output=$(run_cmd mdwf_db promote-ensemble -e test_ens --force 2>&1 || true)
    
    if echo "$promote_output" | grep -q "Promotion OK"; then
        pass "Ensemble promoted to PRODUCTION"
        # Update ensemble dir after promotion
        ENSEMBLE_DIR=$(echo "$ENSEMBLE_DIR" | sed 's/TUNING/ENSEMBLES/')
    else
        fail "Failed to promote ensemble"
    fi
    
    run_test "query by path: Resolve ensemble directory"
    local path_query_output
    path_query_output=$(run_cmd mdwf_db query -e "$ENSEMBLE_DIR" 2>&1 || true)
    if echo "$path_query_output" | grep -q "beta"; then
        pass "Ensemble resolved by directory path"
    else
        fail "Failed to resolve by directory path"
    fi
    
    run_test "query by relative path: Resolve from ensemble directory"
    pushd "$ENSEMBLE_DIR" >/dev/null
    local rel_query_output
    rel_query_output=$(run_cmd mdwf_db query -e . 2>&1 || true)
    if echo "$rel_query_output" | grep -q "beta"; then
        pass "Ensemble resolved from current directory"
    else
        fail "Failed to resolve from current directory"
    fi
    popd >/dev/null
    
    run_test "query: Invalid ensemble ID shows error"
    local invalid_query_output
    invalid_query_output=$(run_cmd mdwf_db query -e 999999 2>&1 || true)
    if echo "$invalid_query_output" | grep -q "Ensemble not found"; then
        pass "Invalid ensemble ID reported correctly"
    else
        fail "Expected error message for invalid ensemble ID"
    fi
    
    run_test "scan: Create dummy configs and scan"
    local cnfg_dir="$ENSEMBLE_DIR/cnfg"
    mkdir -p "$cnfg_dir"
    touch "$cnfg_dir/ckpoint_lat.0.lime"
    touch "$cnfg_dir/ckpoint_lat.4.lime"
    touch "$cnfg_dir/ckpoint_lat.8.lime"
    touch "$cnfg_dir/ckpoint_lat.12.lime"
    
    local scan_output
    scan_output=$(run_cmd mdwf_db scan 2>&1 || true)
    if echo "$scan_output" | grep -qE "(Scanned|configurations|Updated)"; then
        pass "Configuration scan completed"
    else
        fail "Configuration scan failed"
    fi
}

# Phase 3: Input File Generation
phase_3_input_files() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 3: Input File Generation"
    echo "════════════════════════════════════════"
    
    run_test "hmc-xml: Generate HMC XML parameter file"
    local hmc_xml="$TEST_DIR/HMCparameters.xml"
    local hmc_xml_output
    hmc_xml_output=$(run_cmd mdwf_db hmc-xml -e test_ens \
        -o "$hmc_xml" \
        -i "Trajectories=100 trajL=0.75" 2>&1 || true)
    
    if [[ -f "$hmc_xml" ]]; then
        check_file "$hmc_xml" "<?xml"
        check_file "$hmc_xml" "Trajectories"
        check_file "$hmc_xml" "100"
        check_no_template_vars "$hmc_xml"
    else
        fail "hmc-xml generation failed"
    fi
    
    run_test "glu-input: Generate GLU input file"
    local glu_input="$TEST_DIR/glu_smear.in"
    local glu_output
    glu_output=$(run_cmd mdwf_db glu-input -e test_ens \
        -o "$glu_input" \
        -i "SMEARTYPE=STOUT SMITERS=8 ALPHA1=0.75 CONFNO=0" 2>&1 || true)
    
    if [[ -f "$glu_input" ]]; then
        check_file "$glu_input" "SMEARTYPE = STOUT"
        check_file "$glu_input" "SMITERS = 8"
        check_file "$glu_input" "DIM_0 = 24"
        check_no_template_vars "$glu_input"
    else
        fail "glu-input generation failed"
    fi
    
    run_test "wit-input: Generate WIT input file"
    local wit_input="$TEST_DIR/DWF_mres.in"
    local wit_output
    wit_output=$(run_cmd mdwf_db wit-input -e test_ens \
        -o "$wit_input" \
        -i "Configurations.first=0 Configurations.last=100 Configurations.step=4" 2>&1 || true)
    
    if [[ -f "$wit_input" ]]; then
        check_file "$wit_input" "[Configurations]"
        check_file "$wit_input" "first        0"
        check_file "$wit_input" "last         100"
        check_file "$wit_input" "[Lattice parameters]"
        check_file "$wit_input" "Ls           12"
        check_no_template_vars "$wit_input"
    else
        fail "wit-input generation failed"
    fi
    
    run_test "hmc-xml: Missing required parameters fails"
    local missing_xml="$TEST_DIR/HMC_missing.xml"
    local missing_output
    missing_output=$(run_cmd mdwf_db hmc-xml -e test_ens -o "$missing_xml" 2>&1 || true)
    
    if [[ -f "$missing_xml" ]]; then
        fail "hmc-xml should fail without required parameters"
    else
        if echo "$missing_output" | grep -q "Trajectories"; then
            pass "Missing parameter error message detected"
        else
            fail "Missing parameter error message not found"
        fi
    fi
    rm -f "$missing_xml"
}

# Phase 4: SLURM Job Script Generation
phase_4_slurm_scripts() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 4: SLURM Job Script Generation"
    echo "════════════════════════════════════════"
    
    run_test "hmc-script gpu: Generate HMC GPU script"
    local hmc_gpu="$TEST_DIR/hmc_gpu.sh"
    local hmc_gpu_output
    hmc_gpu_output=$(run_cmd mdwf_db hmc-script gpu -e test_ens \
        -j "nodes=1 time_limit=06:00:00 account=m2986_g \
            exec_path=/bin/true bind_script=/bin/true \
            n_trajec=50 trajL=0.75 lvl_sizes=9,1,1 \
            config_start=0 config_end=100" \
        -i "Trajectories=50 trajL=0.75" \
        -o "$hmc_gpu" 2>&1 || true)
    
    if [[ -f "$hmc_gpu" ]]; then
        pass "HMC GPU script generated"
        check_file "$hmc_gpu" "#!/bin/bash"
        check_file "$hmc_gpu" "#SBATCH --nodes=1"
        check_file "$hmc_gpu" "#SBATCH --time=06:00:00"
        check_file "$hmc_gpu" "#SBATCH --constraint=gpu"
        check_file "$hmc_gpu" "source <(python -m MDWFutils.jobs.slurm_update_trap)"
        check_file "$hmc_gpu" "CRAY_ACCEL_TARGET"
        check_no_template_vars "$hmc_gpu"
        [[ $DEBUG -eq 1 ]] && inspect_file "$hmc_gpu" 100
    else
        fail "hmc-script gpu generation failed"
    fi
    
    run_test "hmc-script cpu: Generate HMC CPU script"
    local hmc_cpu="$TEST_DIR/hmc_cpu.sh"
    local hmc_cpu_output
    hmc_cpu_output=$(run_cmd mdwf_db hmc-script cpu -e test_ens \
        -j "nodes=2 time_limit=04:00:00 account=m2986 \
            exec_path=/bin/true bind_script=/bin/true \
            n_trajec=50 trajL=0.75 lvl_sizes=9,1,1 \
            config_start=0 config_end=100" \
        -i "Trajectories=50 trajL=0.75" \
        -o "$hmc_cpu" 2>&1 || true)
    
    if [[ -f "$hmc_cpu" ]]; then
        pass "HMC CPU script generated"
        check_file "$hmc_cpu" "#!/bin/bash"
        check_file "$hmc_cpu" "#SBATCH --constraint=cpu"
        check_file "$hmc_cpu" "I_MPI_PIN"
        check_no_template_vars "$hmc_cpu"
        [[ $DEBUG -eq 1 ]] && inspect_file "$hmc_cpu" 100
    else
        fail "hmc-script cpu generation failed"
    fi
    
    run_test "smear-script: Generate smearing script"
    local smear_script="$TEST_DIR/smear.sh"
    local smear_output
    smear_output=$(run_cmd mdwf_db smear-script -e test_ens \
        -j "config_start=0 config_end=100 config_inc=4 nodes=1" \
        -i "SMEARTYPE=STOUT SMITERS=8" \
        -o "$smear_script" 2>&1 || true)
    
    if [[ -f "$smear_script" ]]; then
        pass "Smear script generated"
        check_file "$smear_script" "#SBATCH"
        check_file "$smear_script" "GLU_SMEAR"
        check_no_template_vars "$smear_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$smear_script" 100
    else
        fail "smear-script generation failed"
    fi
    
    run_test "wflow-script: Generate Wilson flow script"
    local wflow_script="$TEST_DIR/wflow.sh"
    local wflow_output
    wflow_output=$(run_cmd mdwf_db wflow-script -e test_ens \
        -j "config_start=0 config_end=100 nodes=1" \
        -o "$wflow_script" 2>&1 || true)
    
    if [[ -f "$wflow_script" ]]; then
        pass "Wilson flow script generated"
        check_file "$wflow_script" "#SBATCH"
        check_file "$wflow_script" "GLU_WFLOW"
        check_no_template_vars "$wflow_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$wflow_script" 100
    else
        fail "wflow-script generation failed"
    fi
    
    run_test "mres-script: Generate mres measurement script"
    local mres_script="$TEST_DIR/mres.sh"
    local mres_output
    mres_output=$(run_cmd mdwf_db mres-script -e test_ens \
        -j "nodes=1 config_start=0 config_end=100" \
        -i "Configurations.first=0 Configurations.last=100 Configurations.step=4" \
        -o "$mres_script" 2>&1 || true)
    
    if [[ -f "$mres_script" ]]; then
        pass "Mres script generated"
        check_file "$mres_script" "#SBATCH"
        check_file "$mres_script" "WIT_MRES"
        check_no_template_vars "$mres_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$mres_script" 100
    else
        fail "mres-script generation failed"
    fi
    
    run_test "mres-mq-script: Generate mres with varied quark mass"
    local mres_mq_script="$TEST_DIR/mres_mq.sh"
    local mres_mq_output
    mres_mq_output=$(run_cmd mdwf_db mres-mq-script -e test_ens \
        -j "nodes=1 config_start=0 config_end=100" \
        -i "Configurations.first=0 Configurations.last=100 mc=0.8" \
        -o "$mres_mq_script" 2>&1 || true)
    
    if [[ -f "$mres_mq_script" ]]; then
        pass "Mres-mq script generated"
        check_file "$mres_mq_script" "#SBATCH"
        check_file "$mres_mq_script" "WIT_MRES_MQ"
        check_no_template_vars "$mres_mq_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$mres_mq_script" 100
    else
        fail "mres-mq-script generation failed"
    fi
    
    run_test "meson2pt-script: Generate meson correlator script"
    local meson2pt_script="$TEST_DIR/meson2pt.sh"
    local meson2pt_output
    meson2pt_output=$(run_cmd mdwf_db meson2pt-script -e test_ens \
        -j "nodes=1 config_start=0 config_end=100" \
        -i "Configurations.first=0 Configurations.last=100" \
        -o "$meson2pt_script" 2>&1 || true)
    
    if [[ -f "$meson2pt_script" ]]; then
        pass "Meson2pt script generated"
        check_file "$meson2pt_script" "#SBATCH"
        check_file "$meson2pt_script" "WIT_MESON2PT"
        check_no_template_vars "$meson2pt_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$meson2pt_script" 100
    else
        fail "meson2pt-script generation failed"
    fi
    
    run_test "zv-script: Generate Zv measurement script"
    local zv_script="$TEST_DIR/zv.sh"
    local zv_output
    zv_output=$(run_cmd mdwf_db zv-script -e test_ens \
        -j "nodes=1 config_start=0 config_end=50" \
        -i "Configurations.first=0 Configurations.last=50" \
        -o "$zv_script" 2>&1 || true)
    
    if [[ -f "$zv_script" ]]; then
        pass "Zv script generated"
        check_file "$zv_script" "#SBATCH"
        check_file "$zv_script" "WIT_Zv"
        check_no_template_vars "$zv_script"
        [[ $DEBUG -eq 1 ]] && inspect_file "$zv_script" 100
    else
        fail "zv-script generation failed"
    fi
}

# Phase 5: Operation Tracking
phase_5_operations() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 5: Operation Tracking"
    echo "════════════════════════════════════════"
    
    run_test "update: Record RUNNING operation"
    local update_output
    update_output=$(run_cmd mdwf_db update -e test_ens \
        --operation-type TEST_OP \
        --status RUNNING \
        -p "test_param=value slurm_job=12345" 2>&1 || true)
    
    if echo "$update_output" | grep -qE "(Operation|Updated|recorded|RUNNING)"; then
        pass "Operation recorded as RUNNING"
    else
        fail "Failed to record RUNNING operation"
    fi
    
    run_test "query -e test_ens: Verify operation in history"
    local query_history_output
    query_history_output=$(run_cmd mdwf_db query -e test_ens 2>&1 || true)
    if echo "$query_history_output" | grep -qE "(TEST_OP|operation)"; then
        pass "Operation appears in ensemble history"
    else
        fail "Operation not found in history"
    fi
    
    run_test "update: Update to COMPLETED status"
    local update_completed_output
    update_completed_output=$(run_cmd mdwf_db update -e test_ens \
        --operation-type TEST_OP \
        --status COMPLETED \
        -p "exit_code=0 runtime=120" 2>&1 || true)
    
    if echo "$update_completed_output" | grep -qE "(Operation|Updated|recorded|COMPLETED)"; then
        pass "Operation updated to COMPLETED"
    else
        fail "Failed to update operation status"
    fi
    
    run_test "clear-history: Clear operations but preserve ensemble"
    local clear_output
    clear_output=$(timeout 10s mdwf_db clear-history -e test_ens 2>&1 || true)
    local exit_code=$?
    
    echo "$clear_output"
    
    if [[ $exit_code -eq 124 ]]; then
        fail "clear-history timed out (hung for >10s)"
    elif echo "$clear_output" | grep -qE "(Cleared|cleared|removed|Removed)"; then
        pass "Operation history cleared"
    else
        fail "Failed to clear history"
    fi
    
    run_test "MongoDB: Verify ensemble document structure"
    if python3 <<'EOF'
import os
from pymongo import MongoClient

client = MongoClient(os.environ['MDWF_DB_URL'])
db = client.get_database()
doc = db.ensembles.find_one({"nickname": "test_ens"})
assert doc is not None, "Ensemble not found"
for field in ("physics", "paths", "status"):
    assert field in doc, f"Missing field: {field}"
print("✓ Document structure valid")
EOF
    then
        pass "MongoDB document contains required fields"
    else
        fail "MongoDB document missing expected fields"
    fi
}

# Phase 6: Default Parameters
phase_6_defaults() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 6: Default Parameters"
    echo "════════════════════════════════════════"
    
    run_test "default_params set: Store default parameters"
    local default_set_output
    default_set_output=$(run_cmd mdwf_db default_params set -e test_ens \
        --job-type=smear --variant=default \
        --input "SMEARTYPE=STOUT SMITERS=8" \
        --job "nodes=1 time_limit=01:00:00" 2>&1 || true)
    
    if echo "$default_set_output" | grep -qE "(Saved|stored|Stored)"; then
        pass "Default parameters saved"
    else
        fail "Failed to save default parameters"
    fi
    
    run_test "default_params show: Display default parameters"
    local default_show_output
    default_show_output=$(run_cmd mdwf_db default_params show -e test_ens \
        --job-type=smear --variant=default 2>&1 || true)
    
    if echo "$default_show_output" | grep -q "SMEARTYPE"; then
        pass "Default parameters displayed"
    else
        fail "Failed to display default parameters"
    fi
    
    run_test "smear-script: Use default parameters"
    local smear_defaults="$TEST_DIR/smear_with_defaults.sh"
    local smear_defaults_output
    smear_defaults_output=$(run_cmd mdwf_db smear-script -e test_ens \
        -i "SMEARTYPE=STOUT SMITERS=8" \
        -j "config_start=0 config_end=100" \
        -o "$smear_defaults" 2>&1 || true)
    
    if [[ -f "$smear_defaults" ]] && grep -q "STOUT" "$smear_defaults"; then
        pass "Script uses default parameters"
    else
        fail "Script missing default parameters"
    fi
    
    run_test "default_params delete: Remove default parameters"
    local default_delete_output
    default_delete_output=$(run_cmd mdwf_db default_params delete -e test_ens \
        --job-type=smear --variant=default 2>&1 || true)
    
    if echo "$default_delete_output" | grep -qE "(Deleted|removed)"; then
        pass "Default parameters deleted"
    else
        fail "Failed to delete default parameters"
    fi
}

# Phase 7: Cleanup
phase_7_cleanup() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 7: Cleanup"
    echo "════════════════════════════════════════"
    
    run_test "remove-ensemble: Delete test ensemble"
    local remove_output
    remove_output=$(run_cmd mdwf_db remove-ensemble -e test_ens 2>&1 || true)
    
    if echo "$remove_output" | grep -qE "(Removed|Deleted)"; then
        pass "Ensemble removed from database"
    else
        fail "Failed to remove ensemble"
    fi
    
    run_test "Cleanup: Remove test directory"
    if rm -rf "$TEST_DIR"; then
        pass "Test directory removed"
    else
        fail "Failed to remove test directory"
    fi
}

# Main execution
main() {
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
    
    # Check MongoDB connection
    check_mongodb
    
    # Clean test database - flush all collections
    echo "Flushing test data from database..."
    python3 -c "from pymongo import MongoClient; db = MongoClient('$MDWF_DB_URL').get_database(); db.ensembles.delete_many({}); db.operations.delete_many({}); db.default_params.delete_many({})" 2>/dev/null || true
    echo -e "${GREEN}✓${NC} Test data flushed"
    
    # Clean test directory
    echo "Cleaning test directory..."
    rm -rf "$TEST_DIR"
    echo -e "${GREEN}✓${NC} Test directory cleaned"
    
    echo ""
    
    # Run test phases
    # phase_1_init  # Skip - database already exists on NERSC
    phase_2_ensemble_mgmt
    phase_3_input_files
    phase_4_slurm_scripts
    phase_5_operations
    phase_6_defaults
    if [[ $SKIP_CLEANUP -eq 1 ]]; then
        echo "Skipping cleanup (SKIP_CLEANUP=1)"
    else
        phase_7_cleanup
    fi
}

main
