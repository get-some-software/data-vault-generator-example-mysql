# Data Vault Generator Example (MySQL)

## Installation


1. **Install python and requirements**
   
   ```
   sudo dnf module list python38
   sudo dnf module enable -y python38
   sudo dnf install -y python38 python38-pip python38-devel
   python3.8 -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   pip install mysql-connector-python
   python3.8 -m pip install requests
   python3.8 -m pip install pytz
   ```
   
2. **Create an environment file**

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
   export MYSQL_PASSWORD="X"
   export MYSQL_HOST="10.0.0.0"
   export EOD_USERNAME="your_eod_username"
   export EOD_PASSWORD="your_eod_password"

   ```

3. **Install dependencies**

   ```bash
   sudo dnf install mysql-devel python3-devel
   sudo pip3 install mysql-connector-python
   ```

## Usage

1. Source the environment variables:

   ```bash
   . setEnv.sh
   ```

2. set venv:

   ```bash
   source .venv/bin/activate
   ```

3. Start the application:

   ```bash
   sh start.sh
   ```
