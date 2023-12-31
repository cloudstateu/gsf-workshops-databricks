# Databricks Labs

## Lab 1: Installing Databricks CLI

### Objective
Learn to install Databricks CLI and verify its installation.

### Steps
1. Verify the installation of Python and Pip.
2. Install Databricks CLI using the `pip install databricks-cli` command.
3. Verify the installation by checking the version of Databricks CLI.
4. Understand the process of adding Databricks to the Path environment variable.


## Lab 2: Generating a Token in Databricks

### Objective
Learn to generate an access token in Databricks for authentication purposes.

### Steps
1. Log in to your Databricks workspace.
2. Navigate to 'User Settings' and then to the 'Developer' section.
3. Generate a new token with a specified description and expiry period.
4. Practice secure storage of the generated token.

## Lab 3: Connecting with Databricks Using a Token

### Objective
Configure the Databricks CLI to use an access token for authentication.

### Steps
1. Open the terminal or command line.
2. Run the command `databricks configure --token` to start the configuration process.
3. Enter the URL of your Databricks workspace and the generated token when prompted.

## Lab 4: Databricks CLI Configuration for Multiple Environments

### Objective
Learn to set up and use multiple profiles (like 'prod' and 'test') in Databricks CLI.

### Steps
1. Locate and open the `.databrickscfg` configuration file.
2. Edit the file to add different profiles (prod, test) with corresponding host and token details.
3. Practice using these profiles with the `--profile` flag in CLI commands.


## Lab 5: Databricks CLI Configuration for Multiple Environments

### Objective
Learn to manage multiple environments in Databricks CLI, such as production and testing environments.

### Steps
1. **Configuration File Location**: Identify the location of the Databricks CLI configuration file.
   - For Unix-based Systems: `~/.databrickscfg`.
   - For Windows: `C:\\Users\\<YourUsername>\\.databrickscfg`.
2. **Editing the Configuration File**: Open the `.databrickscfg` file in a text editor.
3. **File Format and Profile Setup**: Modify the file to include different profiles. Use the INI file format to structure the file with profiles like `[prod]` and `[test]`.
   - **Prod Profile**: Set the host and token for your production environment under `[prod]`.
   - **Test Profile**: Set the host and token for your test environment under `[test]`.
   - **Default Profile**: Optionally set a default profile under `[DEFAULT]`.
4. **Using Profiles in CLI Commands**: Learn to switch between profiles using the `--profile` flag, e.g., `databricks --profile prod <command>`.
