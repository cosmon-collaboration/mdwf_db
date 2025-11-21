# MongoDB CLI Test Suite

Comprehensive testing framework for the MDWF database management tool CLI commands on NERSC Perlmutter.

## Prerequisites

### MongoDB on NERSC

The test suite requires access to NERSC's MongoDB service:
- Must run on **perlmutter-p1.nersc.gov** (Phase 1 login nodes)
- Database: `mdwf_ensembles` on `mongodb05.nersc.gov`
- Admin credentials configured in `config/admin.env`

### Python Dependencies

```bash
# On Perlmutter
module load python mongodb
pip install --user pymongo jinja2 pyyaml pydantic
```

## Running Tests on Perlmutter

### Setup (one-time)

```bash
# SSH to Phase 1 login nodes
ssh perlmutter-p1.nersc.gov

# Clone/pull the code
cd ~
git clone <your-repo> mdwf_db
cd mdwf_db

# Install dependencies
module load python mongodb
pip install --user pymongo jinja2 pyyaml pydantic
pip install --user -e .

# Configure credentials
source config/admin.env
```

### Run Tests

```bash
cd ~/mdwf_db
source config/admin.env
./test/test_mongodb_cli.sh
```

## Test Coverage

The test suite runs 26 tests across 6 phases:

### Phase 1: SKIPPED
- Database already exists on NERSC (init-db not needed)

### Phase 2: Ensemble Management (8 tests)
- `add-ensemble` - Create test ensemble
- `query` - List all ensembles  
- `query -e <id>` - Show detailed ensemble info
- `nickname` - Set ensemble nickname
- `query -e <nickname>` - Resolve ensemble by nickname
- `promote-ensemble` - Promote from TUNING to PRODUCTION
- `scan` - Scan configuration directory

### Phase 3: Input File Generation (3 tests)
- `hmc-xml` - Generate HMC XML parameter file
- `glu-input` - Generate GLU input file
- `wit-input` - Generate WIT input file

### Phase 4: SLURM Job Script Generation (8 tests)
- `hmc-script gpu` - Generate HMC GPU job script
- `hmc-script cpu` - Generate HMC CPU job script
- `smear-script` - Generate smearing job script
- `wflow-script` - Generate Wilson flow job script
- `mres-script` - Generate mres measurement script
- `mres-mq-script` - Generate mres with varied quark mass
- `meson2pt-script` - Generate meson correlator script
- `zv-script` - Generate Zv measurement script

### Phase 5: Operation Tracking (3 tests)
- `update` (RUNNING) - Record operation start
- `query` (verify operation) - Check operation in history
- `update` (COMPLETED) - Update operation status
- `clear-history` - Clear operation history

### Phase 6: Default Parameters (3 tests)
- `default_params set` - Store default parameters
- `default_params show` - Display default parameters
- `smear-script` (with params) - Generate with explicit params
- `default_params delete` - Remove default parameters

### Phase 7: Cleanup (1 test)
- `remove-ensemble` - Delete test ensemble
- Flush all collections

**Total: 26 tests**

## Test Validation

Each test validates:

1. **Command Execution**: Exit code is 0
2. **File Creation**: Generated files exist
3. **Content Verification**: Key patterns present in output
4. **Template Rendering**: No unsubstituted `{{ }}` variables
5. **Database Operations**: MongoDB documents created/updated correctly

## Expected Output

```
══════════════════════════════════════════════════════════
MongoDB CLI Test Suite
══════════════════════════════════════════════════════════
MongoDB URL: mongodb://localhost:27017/mdwf_test
Test directory: test_run
══════════════════════════════════════════════════════════
Checking MongoDB connection...
✓ MongoDB connection successful
Cleaning test database...
✓ Test database cleaned
Cleaning test directory...
✓ Test directory cleaned

════════════════════════════════════════════
PHASE 1: Database Initialization
════════════════════════════════════════════

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 1: init-db: Create directory structure
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tests run:    32
Tests passed: 32
Tests failed: 0
All tests passed!
```

## Troubleshooting

### MongoDB Connection Issues

```bash
# Check if MongoDB is running
docker ps | grep mongo
# or
brew services list | grep mongodb

# Test connection manually
python3 -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/').server_info())"
```

### Command Not Found

```bash
# Ensure mdwf_db is in PATH
which mdwf_db

# If not, install in development mode
pip install -e /Users/wyatt/Development/mdwf_db
```

### Permission Errors

```bash
# Ensure test script is executable
chmod +x test/test_mongodb_cli.sh

# Ensure MongoDB has write permissions
# Check MongoDB logs for permission issues
```

### Test Failures

If tests fail:

1. Check the test output for specific failure messages
2. Verify MongoDB is accessible and has write permissions
3. Ensure all Python dependencies are installed
4. Check that `MDWF_DB_URL` points to a test database (not production!)
5. Review generated files in `test_run/` directory for debugging

## Cleaning Up

The test script automatically cleans up after itself, but you can manually clean:

```bash
# Drop test database
python3 -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27017/').drop_database('mdwf_test')"

# Remove test directory
rm -rf test_run/

# Stop MongoDB Docker container
docker stop mongodb-test
docker rm mongodb-test
```

## Development

To add new tests:

1. Add a new `run_test` call in the appropriate phase function
2. Use helper functions: `pass()`, `fail()`, `check_file()`, `check_no_template_vars()`
3. Follow the existing pattern for consistency
4. Ensure cleanup in phase_7_cleanup()

## Running Tests on Perlmutter (NERSC)

The test suite can run on NERSC's Perlmutter supercomputer with the NERSC MongoDB service.

### Prerequisites

1. **SSH Tunnel Configuration**

Add to your `~/.ssh/config` on your local machine:

```
Host mongo-tunnel
  Hostname dtn01.nersc.gov
  User your_nersc_username
  LocalForward localhost:27017 mongodb05.nersc.gov:27017
```

2. **Python Environment on Perlmutter**

```bash
# SSH to Perlmutter
ssh perlmutter.nersc.gov

# Load Python module
module load python

# Install dependencies
pip install --user pymongo jinja2 pyyaml pydantic

# Install mdwf_db CLI
cd ~/mdwf_db
pip install --user -e .
```

### Setup MongoDB Connection

1. **Configure credentials** (one-time setup):

```bash
# Copy your admin.env file
cp config/admin.env ~/.mdwf/admin.env
chmod 600 ~/.mdwf/admin.env

# Source it
source ~/.mdwf/admin.env
```

2. **Start SSH tunnel** (in a separate terminal/tmux session):

```bash
# On your local machine or in a tmux session
ssh mongo-tunnel
# Keep this running
```

### Running the Tests

```bash
# On Perlmutter
cd ~/mdwf_db

# Load credentials
source ~/.mdwf/admin.env

# Verify connection
python3 -c "import os; from pymongo import MongoClient; print('Connected:', MongoClient(os.environ['MDWF_DB_URL']).admin.command('ping'))"

# Run test suite
./test/test_mongodb_cli.sh
```

### Expected Results

- **Tests run**: 26 (Phase 1 init-db is skipped since database already exists)
- **Expected**: All 26 tests pass
- **Test creates**: Ensemble with nickname "test_ens"
- **Test cleans up**: Flushes all collections at end (database remains but is empty)

### Important Notes for NERSC

1. **Database cannot be dropped**: NERSC provides one `mdwf_ensembles` database
   - Tests flush collections (delete all documents) instead of dropping database
   - Database structure remains intact

2. **No init-db test**: Phase 1 is skipped because database already exists

3. **Test directory**: Use `$SCRATCH` or home directory for test files
   ```bash
   cd $SCRATCH
   git clone <your-repo> mdwf_db
   ```

4. **If tests fail mid-run**: Manually clean up test ensemble
   ```bash
   mdwf_db remove-ensemble -e test_ens
   ```

5. **SSH tunnel required**: MongoDB is only accessible from within NERSC network
   - Use SSH tunnel from local machine, or
   - Run tests from NERSC login/compute nodes directly

### Troubleshooting on Perlmutter

**Connection refused**:
```bash
# Check if tunnel is running
ps aux | grep ssh | grep mongo-tunnel

# Restart tunnel if needed
ssh mongo-tunnel
```

**Module not found**:
```bash
# Ensure Python module loaded
module list | grep python
module load python

# Check pip install path
pip show pymongo
```

**Permission denied for database operations**:
- Verify you're using admin account credentials
- Check `MDWF_DB_URL` is set: `echo $MDWF_DB_URL`

## Notes

- Tests are idempotent (can be run multiple times)
- Each run starts by flushing all collections
- SQLite backend is intentionally not tested (read-only, migration-only)
- All tests use MongoDB exclusively
- Generated scripts are validated but not executed


