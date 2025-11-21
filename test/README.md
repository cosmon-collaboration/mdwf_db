# MongoDB CLI Test Suite

Comprehensive testing framework for the MDWF database management tool CLI commands.

## Prerequisites

### 1. MongoDB

The test suite requires a running MongoDB instance:

```bash
# Using Docker (recommended for testing)
docker run -d -p 27017:27017 --name mongodb-test mongo:latest

# Or install locally
# macOS:
brew install mongodb-community
brew services start mongodb-community

# Linux:
sudo systemctl start mongod
```

### 2. Python Dependencies

Ensure all required packages are installed:

```bash
pip install pymongo jinja2 pyyaml pydantic
```

Or install the package in development mode:

```bash
cd ~/mdwf_db  # or wherever you cloned it
pip install -e .
```

## Running Tests

### Basic Test Run

```bash
# From the project root
./test/test_mongodb_cli.sh
```

### Custom MongoDB URL

```bash
# Specify a custom MongoDB connection string
export MDWF_DB_URL=mongodb://localhost:27017/mdwf_test
./test/test_mongodb_cli.sh
```

### Using Remote MongoDB

```bash
# For remote MongoDB (e.g., Atlas)
export MDWF_DB_URL="mongodb+srv://user:pass@cluster.mongodb.net/mdwf_test"
./test/test_mongodb_cli.sh
```

## Test Coverage

The test suite exercises all 20 CLI commands across 7 phases:

### Phase 1: Database Initialization (1 test)
- `init-db` - Create directory structure and verify MongoDB connection

### Phase 2: Ensemble Management (10 tests)
- `add-ensemble` - Create test ensemble with physics parameters
- `query` - List all ensembles
- `query -e <id>` - Show detailed ensemble info
- `query -e <path>` - Resolve ensemble by directory path
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

### Phase 5: Operation Tracking (4 tests)
- `update` (RUNNING) - Record operation start
- `query` (verify operation) - Check operation in history
- `update` (COMPLETED) - Update operation status
- `clear-history` - Clear operation history

### Phase 6: Default Parameters (4 tests)
- `default_params save` - Store default parameters
- `default_params show` - Display default parameters
- `smear-script` (with defaults) - Use stored defaults
- `default_params delete` - Remove default parameters

### Phase 7: Cleanup (2 tests)
- `remove-ensemble` - Delete test ensemble
- Directory cleanup - Remove test files

**Total: 30+ test assertions**

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


