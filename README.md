# MDWF Database Tool

## Overview
The MDWF Database Tool is a command-line utility designed to manage and automate workflows for lattice QCD simulations. It provides a robust interface for generating SLURM scripts, managing ensemble parameters, and automating common tasks in the MDWF workflow.

**Note:** The examples in this README are run from the `test_cli` folder. On Perlmutter, commands should generally be run from the `mdwf` folder.

## Installation
To install the MDWF Database Tool, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/smithwya/mdwf_db.git
   cd mdwf_db
   ```

2. Install the package:
   ```bash
   pip install -e .
   ```

## Usage
The MDWF Database Tool provides several commands for managing your workflow:

### Initialize the Database
To initialize a new MDWF database in your working directory, use the following command:

**Command:**
```bash
mdwf_db init-db
```

**Available Options:**
- `--db-file <path>`: Path to the database file to create (default: `mdwf_ensembles.db` in the current directory)
- `--base-dir <path>`: Root directory for the database and ensemble folders (default: current directory)

**Sample Command:**
```bash
mdwf_db init-db
```

**Expected Output:**
```
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli/TUNING
Ensured directory: /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES
init_database returned: True
```

**Files and Directories Created:**
- `mdwf_ensembles.db` (SQLite database file)
- `TUNING/` (directory for tuning ensembles)
- `ENSEMBLES/` (directory for production ensembles)

### Add an Ensemble
To add a new ensemble to the database, use the following command:

**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```

**Available Options:**
- `-p <params>`: Space-separated key=value pairs for ensemble parameters (required)
- `-s <status>`: Ensemble status (TUNING or PRODUCTION) (required)
- `--description <text>`: Description of the ensemble (optional)

**Sample Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```

**Output:**
```
Ensemble added: ID=1
```

**Resulting Directory Structure:**
```
TUNING/
└── b6.0/
    └── b1.8Ls24/
        └── mc0.85/
            └── ms0.07/
                └── ml0.02/
                    └── L32/
                        └── T64/
                            ├── cnfg/
                            ├── jlog/
                            ├── log_hmc/
                            └── slurm/
```

### Promote an Ensemble
To promote an ensemble, use the following command:

**Command:**
```bash
mdwf_db promote --db-file=/path/to/mdwf_ensembles.db --ensemble-id=<ensemble_id>
```

**Parameters:**
- `--db-file=<db_file>`: Path to the database file.
- `--ensemble-id=<ensemble_id>`: The ID of the ensemble to promote.

**Sample Command:**
```bash
mdwf_db promote --db-file=/path/to/mdwf_ensembles.db --ensemble-id=1
```

**Expected Output:**
```
Ensemble promoted successfully.
```

### Print History
To print the history of an ensemble, use the following command:

**Command:**
```bash
mdwf_db query -e <ensemble_id>
```

**Parameters:**
- `-e <ensemble_id>`: The ID of the ensemble to query for history.

**Sample Command:**
```bash
mdwf_db query -e 1
```

**Expected Output:**
```
ID          = 1
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
Status      = TUNING
Created     = 2025-06-11T15:32:53.297052
Description = Test ensemble for workflow
Parameters:
    L = 32
    Ls = 24
    T = 64
    b = 1.8
    beta = 6.0
    mc = 0.85
    ml = 0.02
    ms = 0.07

=== Operation history ===
Op 1: ADD_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:32:53.297052
  Updated: 2025-06-11T15:32:53.297052
```

### Generate Smearing Script for an Ensemble
You can generate a smearing SLURM script for an ensemble using the following command. The script will generate both a GLU input file and an SBATCH script.

**Command:**
```bash
mdwf_db smear-script -e 1 -j "queue=regular config_start=0 config_end=10 mail_user=wyatt@example.com"
```

**Output:**
```
Generated GLU input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in
Wrote smearing SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh
```

**Generated Files:**
- GLU Input File: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in`
- SBATCH Script: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh`

### Generate Meson 2pt Script for an Ensemble
You can generate a meson 2pt SLURM script for an ensemble using the following command. The script will generate both a WIT input file and an SBATCH script.

**Command:**
```bash
mdwf_db meson-2pt -e 1 -j "queue=regular time_limit=1:00:00 nodes=1 cpus_per_task=16 mail_user=wyatt@example.com" -w "Configurations.first=0 Configurations.last=10"
```

**Output:**
```
Generated WIT input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in
Wrote meson 2pt SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh
```

**Generated Files:**
- WIT Input File: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in`
- SBATCH Script: `/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh`

### Generate HMC Script for an Ensemble
You can generate an HMC SLURM script for an ensemble using the following command. The script will prompt for the HMC executable and core binding script if not already set.

**Command:**
```bash
mdwf_db hmc-script -e 1 -a m2986_g -m tepid -j "queue=regular cfg_max=10 mail_user=wyatt@example.com"
```

**Output:**
```
Please enter the path to the HMC executable: test/hmc_exec
Please enter the path to the core binding script: test/bind
Wrote HMC sbatch -> /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

**Generated Script Location:**
```
/Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

### Manage Ensemble Parameters
Use the `mdwf_db` command to update and manage ensemble parameters.

**Command:**
```bash
mdwf_db update --db-file=<db_file> --ensemble-id=<ensemble_id> --operation-type=<operation> --status=<status> --params=<params>
```

**Parameters:**
- `--db-file=<db_file>`: Path to the database file.
- `--ensemble-id=<ensemble_id>`: The ID of the ensemble to update.
- `--operation-type=<operation>`: The type of operation (e.g., "WIT_MESON2PT").
- `--status=<status>`: The status to set (e.g., "RUNNING", "COMPLETED", "FAILED").
- `--params=<params>`: Additional parameters to include.

**Sample Command:**
```bash
mdwf_db update --db-file=/path/to/mdwf_ensembles.db --ensemble-id=1 --operation-type="WIT_MESON2PT" --status="RUNNING" --params="slurm_job=12345 host=node1"
```

**Expected Output:**
```
Ensemble parameters updated successfully.
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Workflow Log
Below is a log of all operations performed during the testing and documentation of the MDWF Database Tool.

### Initialize Database
**Command:**
```bash
mdwf_db init --db-file=mdwf_ensembles.db
```
**Output:**
```
Database initialized at mdwf_ensembles.db
```

### Add First Ensemble
**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=24 mc=0.85 ms=0.07 ml=0.02 L=32 T=64" -s TUNING --description "Test ensemble for workflow"
```
**Output:**
```
Ensemble added: ID=1
```

### Query All Ensembles
**Command:**
```bash
mdwf_db query
```
**Output:**
```
[1] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
```

### Add Second Ensemble
**Command:**
```bash
mdwf_db add-ensemble -p "beta=6.0 b=1.8 Ls=16 mc=0.80 ms=0.06 ml=0.01 L=24 T=48" -s TUNING --description "Second test ensemble"
```
**Output:**
```
Ensemble added: ID=2
```

### Query All Ensembles Again
**Command:**
```bash
mdwf_db query
```
**Output:**
```
[1] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64
[2] (TUNING) /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
```

### Promote an Ensemble
**Command:**
```bash
mdwf_db promote-ensemble -e 2
```
**Output:**
```
Promote ensemble 2:
  from /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
    to /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Proceed? (y/N) y
Created operation 1: Created
Promotion OK
```

### Query a Single Ensemble (Detailed View)
**Command:**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
```

### Update an Ensemble
**Command:**
```bash
mdwf_db update --ensemble-id=2 --operation-type=NOTE --status=COMPLETED --params "note=Testing_update_command"
```
**Output:**
```
Created operation 2: Created
```

### Query the Ensemble After Update
**Command:**
```bash
mdwf_db query -e 2 --detailed
```
**Output:**
```
ID          = 2
Directory   = /Users/wyatt/Development/mdwf_db/test_cli/ENSEMBLES/b6.0/b1.8Ls16/mc0.80/ms0.06/ml0.01/L24/T48
Status      = PRODUCTION
Created     = 2025-06-11T15:36:14.231365
Description = Second test ensemble
Parameters:
    L = 24
    Ls = 16
    T = 48
    b = 1.8
    beta = 6.0
    mc = 0.80
    ml = 0.01
    ms = 0.06

=== Operation history ===
Op 1: PROMOTE_ENSEMBLE [COMPLETED]
  Created: 2025-06-11T15:36:27.298971
  Updated: 2025-06-11T15:36:27.298971
Op 2: NOTE [COMPLETED]
  Created: 2025-06-11T15:37:26.713743
  Updated: 2025-06-11T15:37:26.713743
    note = Testing_update_command
```

### Generate HMC Script
**Command:**
```bash
mdwf_db hmc-script -e 1 -a m2986_g -m tepid -j "queue=regular cfg_max=10 mail_user=wyatt@example.com"
```
**Output:**
```
Please enter the path to the HMC executable: test/hmc_exec
Please enter the path to the core binding script: test/bind
Wrote HMC sbatch -> /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/hmc_1_tepid.sbatch
```

### Generate Smearing Script
**Command:**
```bash
mdwf_db smear-script -e 1 -j "queue=regular config_start=0 config_end=10 mail_user=wyatt@example.com"
```
**Output:**
```
Generated GLU input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/cnfg_STOUT8/glu_smear.in
Wrote smearing SBATCH script → /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/slurm/glu_smear_STOUT8_0_10.sh
```

### Generate Meson 2pt Script
**Command:**
```bash
mdwf_db meson-2pt -e 1 -j "queue=regular time_limit=1:00:00 nodes=1 cpus_per_task=16 mail_user=wyatt@example.com" -w "Configurations.first=0 Configurations.last=10"
```
**Output:**
```
Generated WIT input file: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/DWF.in
Generated WIT SBATCH script: /Users/wyatt/Development/mdwf_db/test_cli/TUNING/b6.0/b1.8Ls24/mc0.85/ms0.07/ml0.02/L32/T64/meson2pt/meson2pt_0_10.sh
