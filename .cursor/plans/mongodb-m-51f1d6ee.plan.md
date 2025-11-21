<!-- 51f1d6ee-407a-446f-85bd-be0108cddf65 0469af6f-718d-4851-a951-d880b4f473dd -->
# Perlmutter Test Readiness Audit

## Issues Found

### Issue 1: Missing @ symbol in admin.env connection string

**Location**: `config/admin.env` line 6

**Problem**: The connection string is missing the `@` symbol before the hostname:

```
mongodb://mdwf_ensembles_admin:a+place+for+everything+and+everything+in+its+place"@mongodb05.nersc.gov...
```

The closing quote after the password should be removed, and the connection string format should be:

```
mongodb://username:password@hostname:port/database
```

**Fix**: Change line 6 to:

```bash
export MDWF_DB_URL="mongodb://mdwf_ensembles_admin:a+place+for+everything+and+everything+in+its+place@mongodb05.nersc.gov:27017/mdwf_ensembles?authSource=admin"
```

### Issue 2: Test script database cleanup needs NERSC consideration

**Location**: `test/test_mongodb_cli.sh` line 508-510

**Problem**: The cleanup function drops the entire `mdwf_test` database, but on NERSC you're using `mdwf_ensembles` (production database name). The test script currently:

1. Uses `MDWF_DB_URL` which points to `mdwf_ensembles`
2. But tries to drop `mdwf_test` database (hardcoded)

**Current code**:

```bash
if [[ "$MDWF_DB_URL" != "mongomock://"* ]]; then
    echo "Cleaning up test database..."
    python3 -c "from pymongo import MongoClient; client = MongoClient('$MDWF_DB_URL'); client.drop_database('mdwf_test')" 2>/dev/null || true
fi
```

**Issue**: This will NOT clean up the test data from `mdwf_ensembles` because it's dropping the wrong database.

**Two possible solutions**:

**Option A**: Use a separate test database URL for testing

- Create a test database `mdwf_test` on NERSC MongoDB
- Test with: `export MDWF_DB_URL="mongodb://user:pass@mongodb05.nersc.gov:27017/mdwf_test?authSource=admin"`
- This is safer and cleaner

**Option B**: Extract database name from connection string

- Parse the database name from `MDWF_DB_URL`
- Use that for cleanup
- Risk: might accidentally drop production data if misconfigured

**Recommendation**: Use Option A - request a separate `mdwf_test` database from NERSC for testing.

### Issue 3: Test README references wrong paths and setup

**Location**: `test/README.md` lines 35-36

**Problem**: Hardcoded local path `/Users/wyatt/Development/mdwf_db` in documentation.

**Fix**: Change to relative paths for portability:

```bash
cd ~/mdwf_db  # or wherever you cloned it
pip install -e .
```

### Issue 4: Missing Perlmutter-specific setup instructions

**Location**: `test/README.md` (needs new section)

**Problem**: No instructions for:

1. Setting up SSH tunnel (per NERSC docs)
2. Loading Python modules on Perlmutter
3. Installing dependencies with `--user` flag
4. Using `$SCRATCH` instead of local directories

**Fix**: Add new section "Running Tests on Perlmutter" with:

- SSH tunnel setup matching NERSC documentation pattern
- Module loading: `module load python`
- User package installation: `pip install --user`
- Environment variable setup for admin account
- Reference to `config/admin.env`

## Validation Checklist

Before running on Perlmutter, verify:

1. **Connection String Format**

   - [ ] No stray quotes in password
   - [ ] Has `@` before hostname
   - [ ] Spaces replaced with `+`
   - [ ] Database name matches target
   - [ ] Format: `mongodb://user:pass@host:port/db?authSource=admin`

2. **Test Database Strategy**

   - [ ] Decide: Use `mdwf_test` or `mdwf_ensembles` for testing?
   - [ ] If `mdwf_ensembles`: Add warning about cleanup
   - [ ] If `mdwf_test`: Request separate test database from NERSC

3. **Perlmutter Environment**

   - [ ] Python module availability: `module avail python`
   - [ ] Install dependencies: `pip install --user pymongo jinja2 pyyaml pydantic`
   - [ ] SSH tunnel configured: `~/.ssh/config` entry for `mongo-tunnel`
   - [ ] Test connection: `python3 -c "from pymongo import MongoClient; MongoClient('$MDWF_DB_URL').admin.command('ping')"`

4. **Test Script Configuration**

   - [ ] `MDWF_DB_URL` set correctly via `source config/admin.env`
   - [ ] Database name in URL matches cleanup expectations
   - [ ] Test directory will be in `$SCRATCH` or writable location

## Files to Modify

1. **config/admin.env** - Fix connection string syntax error
2. **test/test_mongodb_cli.sh** - Extract database name from URL for cleanup (optional, depending on strategy)
3. **test/README.md** - Add Perlmutter-specific instructions
4. **New file: config/PERLMUTTER_SETUP.md** - Create detailed Perlmutter deployment guide

## Recommended Testing Workflow on Perlmutter

```bash
# 1. Transfer code
scp -r mdwf_db perlmutter.nersc.gov:~/

# 2. SSH to Perlmutter
ssh perlmutter.nersc.gov

# 3. Set up environment
module load python
pip install --user pymongo jinja2 pyyaml pydantic

# 4. Set up SSH tunnel (in separate terminal/tmux)
ssh mongo-tunnel  # Keep running

# 5. Source admin credentials
cd ~/mdwf_db
source config/admin.env

# 6. Verify connection
python3 -c "from pymongo import MongoClient; print(MongoClient(os.environ['MDWF_DB_URL']).admin.command('ping'))"

# 7. Run tests
./test/test_mongodb_cli.sh

# 8. Review results
# Expected: 29/29 tests pass
```

## Priority Fixes

**CRITICAL (must fix before testing)**:

1. Fix admin.env connection string (missing @)

**HIGH (should fix before testing)**:

2. Clarify test database strategy (separate mdwf_test vs. production mdwf_ensembles)
3. Add Perlmutter setup documentation

**MEDIUM (can fix after initial test)**:

4. Make test cleanup database-name-aware
5. Update README with portable paths

## Safety Notes

1. **Production Database Testing**: Tests run against `mdwf_ensembles` (production database)
2. Test creates ensemble with nickname "test_ens" and removes it at completion
3. Cleanup is **ensemble-specific**, NOT database-wide - other ensembles are safe
4. If test fails mid-run, manual cleanup: `mdwf_db remove-ensemble -e test_ens`
5. Test ensemble creates files in `test_run/` directory which is cleaned up locally
6. No existing production ensembles are affected by the test suite

### To-dos

- [ ] Fix admin.env connection string - remove stray quote, add @ symbol
- [ ] Determine if using mdwf_test or mdwf_ensembles for testing
- [ ] Update test script cleanup to extract DB name from connection URL
- [ ] Create config/PERLMUTTER_SETUP.md with deployment instructions
- [ ] Update test/README.md with Perlmutter section and portable paths
- [ ] Test MongoDB connection with fixed connection string