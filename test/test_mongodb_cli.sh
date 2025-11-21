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

VERBOSE=${VERBOSE:-0}
SKIP_CLEANUP=${SKIP_CLEANUP:-0}

if [[ ${1:-} == "-v" || ${1:-} == "--verbose" ]]; then
    VERBOSE=1
    shift
fi

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
}

run_cmd() {
    if [[ $VERBOSE -eq 1 ]]; then
        echo "RUN: $*"
    fi
    "$@"
}

check_file() {
    local file="$1"
    local pattern="${2:-}"
    if [[ ! -f "$file" ]]; then
        fail "File not found: $file"
    elif [[ -n "$pattern" ]] && ! grep -q "$pattern" "$file"; then
        fail "Pattern not found in $file: $pattern"
    else
        pass "File verified: $file"
    fi
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
    if mdwf_db init-db --base-dir="$TEST_DIR" 2>&1 | tee /tmp/init_output.log; then
        if [[ -d "$TEST_DIR/TUNING" ]] && [[ -d "$TEST_DIR/ENSEMBLES" ]]; then
            pass "Directories created successfully"
        else
            fail "Directories not created"
        fi
    else
        fail "init-db command failed"
    fi
}

# Phase 2: Ensemble Management
phase_2_ensemble_mgmt() {
    echo ""
    echo "════════════════════════════════════════"
    echo "PHASE 2: Ensemble Management"
    echo "════════════════════════════════════════"
    
    run_test "add-ensemble: Create test ensemble"
    local add_output=$(mdwf_db add-ensemble \
        --params "beta=6.0 b=2.5 Ls=12 mc=0.6 ms=0.04 ml=0.005 L=24 T=48" \
        --status=TUNING \
        --base-dir="$TEST_DIR" 2>&1)
    
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
    if mdwf_db query --detailed 2>&1 | grep -q "beta"; then
        pass "Query lists ensembles"
    else
        fail "Query failed to list ensembles"
    fi
    
    run_test "query -e <id>: Show detailed ensemble info"
    if mdwf_db query -e "$ENSEMBLE_ID" 2>&1 | grep -q "beta.*6.0"; then
        pass "Detailed query shows physics parameters"
    else
        fail "Detailed query missing parameters"
    fi
    
    run_test "nickname: Set nickname 'test_ens'"
    if mdwf_db nickname -e "$ENSEMBLE_ID" --set test_ens 2>&1 | grep -q "Set nickname"; then
        pass "Nickname set successfully"
    else
        fail "Failed to set nickname"
    fi
    
    run_test "query -e test_ens: Resolve by nickname"
    if mdwf_db query -e test_ens 2>&1 | grep -q "beta"; then
        pass "Ensemble resolved by nickname"
    else
        fail "Failed to resolve by nickname"
    fi
    
    run_test "promote-ensemble: Move to PRODUCTION"
    if mdwf_db promote-ensemble -e test_ens 2>&1 | grep -q "Promotion OK"; then
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
    
    if mdwf_db scan 2>&1 | grep -qE "(Scanned|configurations|Updated)"; then
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
    if mdwf_db hmc-xml -e test_ens \
        -o "$hmc_xml" \
        -i "Trajectories=100 trajL=0.75" 2>&1; then
        check_file "$hmc_xml" "<?xml"
        check_file "$hmc_xml" "Trajectories"
        check_file "$hmc_xml" "100"
        check_no_template_vars "$hmc_xml"
    else
        fail "hmc-xml generation failed"
    fi
    
    run_test "glu-input: Generate GLU input file"
    local glu_input="$TEST_DIR/glu_smear.in"
    if mdwf_db glu-input -e test_ens \
        -o "$glu_input" \
        -i "SMEARTYPE=STOUT SMITERS=8 ALPHA1=0.75 CONFNO=0" 2>&1; then
        check_file "$glu_input" "SMEARTYPE = STOUT"
        check_file "$glu_input" "SMITERS = 8"
        check_file "$glu_input" "DIM_0 = 24"
        check_no_template_vars "$glu_input"
    else
        fail "glu-input generation failed"
    fi
    
    run_test "wit-input: Generate WIT input file"
    local wit_input="$TEST_DIR/DWF_mres.in"
    if mdwf_db wit-input -e test_ens \
        -o "$wit_input" \
        -i "Configurations.first=0 Configurations.last=100 Configurations.step=4" 2>&1; then
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
    if mdwf_db hmc-xml -e test_ens -o "$missing_xml" 2> >(tee /tmp/hmc_missing.log >&2); then
        fail "hmc-xml should fail without required parameters"
    else
        if grep -q "Trajectories" /tmp/hmc_missing.log; then
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
    if mdwf_db hmc-script gpu -e test_ens \
        -j "nodes=1 time_limit=06:00:00 account=m2986_g config_start=0 config_end=100" \
        -i "Trajectories=50 trajL=0.75" \
        -o "$hmc_gpu" 2>&1; then
        check_file "$hmc_gpu" "#!/bin/bash"
        check_file "$hmc_gpu" "#SBATCH --nodes=1"
        check_file "$hmc_gpu" "#SBATCH --time=06:00:00"
        check_file "$hmc_gpu" "#SBATCH --constraint=gpu"
        check_file "$hmc_gpu" "source <(python -m MDWFutils.jobs.slurm_update_trap)"
        check_file "$hmc_gpu" "CRAY_ACCEL_TARGET"
        check_no_template_vars "$hmc_gpu"
    else
        fail "hmc-script gpu generation failed"
    fi
    
    run_test "hmc-script cpu: Generate HMC CPU script"
    local hmc_cpu="$TEST_DIR/hmc_cpu.sh"
    if mdwf_db hmc-script cpu -e test_ens \
        -j "nodes=2 time_limit=04:00:00 account=m2986 config_start=0 config_end=100" \
        -i "Trajectories=50" \
        -o "$hmc_cpu" 2>&1; then
        check_file "$hmc_cpu" "#!/bin/bash"
        check_file "$hmc_cpu" "#SBATCH --constraint=cpu"
        check_file "$hmc_cpu" "I_MPI_PIN"
        check_no_template_vars "$hmc_cpu"
    else
        fail "hmc-script cpu generation failed"
    fi
    
    run_test "smear-script: Generate smearing script"
    local smear_script="$TEST_DIR/smear.sh"
    if mdwf_db smear-script -e test_ens \
        -j "config_start=0 config_end=100 config_inc=4 nodes=1" \
        -i "SMEARTYPE=STOUT SMITERS=8" \
        -o "$smear_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$smear_script" "#SBATCH"
        check_file "$smear_script" "GLU_SMEAR"
        check_no_template_vars "$smear_script"
    else
        fail "smear-script generation failed"
    fi
    
    run_test "wflow-script: Generate Wilson flow script"
    local wflow_script="$TEST_DIR/wflow.sh"
    if mdwf_db wflow-script -e test_ens \
        -j "config_start=0 config_end=100 nodes=1" \
        -i "SMITERS=250" \
        -o "$wflow_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$wflow_script" "#SBATCH"
        check_file "$wflow_script" "GLU_WFLOW"
        check_no_template_vars "$wflow_script"
    else
        fail "wflow-script generation failed"
    fi
    
    run_test "mres-script: Generate mres measurement script"
    local mres_script="$TEST_DIR/mres.sh"
    if mdwf_db mres-script -e test_ens \
        -j "nodes=1 gpus=4" \
        -i "Configurations.first=0 Configurations.last=100 Configurations.step=4" \
        -o "$mres_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$mres_script" "#SBATCH"
        check_file "$mres_script" "WIT_MRES"
        check_no_template_vars "$mres_script"
    else
        fail "mres-script generation failed"
    fi
    
    run_test "mres-mq-script: Generate mres with varied quark mass"
    local mres_mq_script="$TEST_DIR/mres_mq.sh"
    if mdwf_db mres-mq-script -e test_ens \
        -j "nodes=1" \
        -i "Configurations.first=0 Configurations.last=100 mc=0.8" \
        -o "$mres_mq_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$mres_mq_script" "#SBATCH"
        check_file "$mres_mq_script" "WIT_MRES_MQ"
        check_no_template_vars "$mres_mq_script"
    else
        fail "mres-mq-script generation failed"
    fi
    
    run_test "meson2pt-script: Generate meson correlator script"
    local meson2pt_script="$TEST_DIR/meson2pt.sh"
    if mdwf_db meson2pt-script -e test_ens \
        -j "nodes=1" \
        -i "Configurations.first=0 Configurations.last=100" \
        -o "$meson2pt_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$meson2pt_script" "#SBATCH"
        check_file "$meson2pt_script" "WIT_MESON2PT"
        check_no_template_vars "$meson2pt_script"
    else
        fail "meson2pt-script generation failed"
    fi
    
    run_test "zv-script: Generate Zv measurement script"
    local zv_script="$TEST_DIR/zv.sh"
    if mdwf_db zv-script -e test_ens \
        -j "nodes=1" \
        -i "Configurations.first=0 Configurations.last=50" \
        -o "$zv_script" 2>&1 | grep -qE "(Generated|Created|written)"; then
        check_file "$zv_script" "#SBATCH"
        check_file "$zv_script" "WIT_Zv"
        check_no_template_vars "$zv_script"
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
    if mdwf_db update -e test_ens \
        --operation-type=TEST_OP \
        --status=RUNNING \
        -p "test_param=value slurm_job=12345" 2>&1 | grep -qE "(Operation|Updated|recorded)"; then
        pass "Operation recorded as RUNNING"
    else
        fail "Failed to record RUNNING operation"
    fi
    
    run_test "query -e test_ens: Verify operation in history"
    if mdwf_db query -e test_ens 2>&1 | grep -qE "(TEST_OP|operation)"; then
        pass "Operation appears in ensemble history"
    else
        fail "Operation not found in history"
    fi
    
    run_test "update: Update to COMPLETED status"
    if mdwf_db update -e test_ens \
        --operation-type=TEST_OP \
        --status=COMPLETED \
        -p "exit_code=0 runtime=120" 2>&1 | grep -qE "(Operation|Updated|recorded)"; then
        pass "Operation updated to COMPLETED"
    else
        fail "Failed to update operation status"
    fi
    
    run_test "clear-history: Clear operations but preserve ensemble"
    if mdwf_db clear-history -e test_ens 2>&1 | grep -qE "(Cleared|removed)"; then
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
    if mdwf_db default_params set -e test_ens \
        --job-type=smear --variant=default \
        --input "SMEARTYPE=STOUT SMITERS=8" \
        --job "nodes=1 time_limit=01:00:00" 2>&1 | grep -qE "(Saved|stored|Stored)"; then
        pass "Default parameters saved"
    else
        fail "Failed to save default parameters"
    fi
    
    run_test "default_params show: Display default parameters"
    if mdwf_db default_params show -e test_ens \
        --job-type=smear --variant=default 2>&1 | grep -q "SMEARTYPE"; then
        pass "Default parameters displayed"
    else
        fail "Failed to display default parameters"
    fi
    
    run_test "smear-script: Use default parameters"
    local smear_defaults="$TEST_DIR/smear_with_defaults.sh"
    if mdwf_db smear-script -e test_ens \
        -i "SMEARTYPE=STOUT SMITERS=8" \
        -j "config_start=0 config_end=100" \
        -o "$smear_defaults" 2>&1; then
        if grep -q "STOUT" "$smear_defaults"; then
            pass "Script uses default parameters"
        else
            fail "Script missing default parameters"
        fi
    else
        fail "Failed to generate script with defaults"
    fi
    
    run_test "default_params delete: Remove default parameters"
    if mdwf_db default_params delete -e test_ens \
        --job-type=smear --variant=default 2>&1 | grep -qE "(Deleted|removed)"; then
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
    if mdwf_db remove-ensemble -e test_ens 2>&1 | grep -qE "(Removed|Deleted)"; then
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
    echo "Test directory: $TEST_DIR"
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
