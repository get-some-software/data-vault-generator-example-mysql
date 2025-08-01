# Data Vault Generator Example (Oracle)

## Installation

1. **Create an environment file**

   Create a file named `setEnv.sh` with the following content:

   ```sh
   export GETSOME_OCI_MTM_PASSWORD="x"
   export GETSOME_OCI_EXN_PASSWORD="x"
   export GETSOME_OCI_MTM_SCHEMA="MTM"
   export GETSOME_OCI_DSN="getsome_low"
   export GETSOME_OCI_CONFIG_DIR=/opt/git/Wallet_GETSOME/
   export GETSOME_OCI_WALLET_LOCATION=$GETSOME_OCI_CONFIG_DIR
   export GETSOME_OCI_WALLET_PASSWORD=
   export GETSOME_GETSOME_EOD_SCHEMA=EXN1
   ```

2. **Install dependencies**

   ```bash
   sudo dnf install mysql-devel python3-devel
   pip3 install mysql-connector-python
   ```

## Usage

1. Source the environment variables:

   ```bash
   . setEnv.sh
   ```

2. Start the application:

   ```bash
   sh start.sh
   ```