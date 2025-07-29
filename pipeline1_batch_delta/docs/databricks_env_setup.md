%md
# Databricks Environment Setup (Azure + CLI + Workspace)

## Prerequisites
- Azure subscription and admin rights for creating Key Vault and Resource Groups
- Databricks workspace provisioned and accessible
- Install required tools:
  - Azure CLI
  - Databricks CLI v0.200.0 or later
  - Git Bash (for working around certain Windows quoting issues)

## 1. Install and Configure Azure CLI
```bash
az login
az account set --subscription "<your-subscription-name>"
```

## 2. Install and Configure Databricks CLI
```bash
pip install databricks-cli
databricks configure --profile <profile-name>
# Example for token-based setup:
# Enter your Databricks host (https://adb-xxxx.azuredatabricks.net)
# Then paste your personal access token
```

## 3. Validate Configuration
```bash
databricks workspace ls --profile <profile-name>
databricks clusters list --profile <profile-name>
```

## 4. Common CLI Troubleshooting
- If both old and new versions of CLI are installed:
```bash
# Set environment variable to prefer newer CLI
set DATABRICKS_CLI_DO_NOT_EXECUTE_NEWER_VERSION=1
```
- Ensure CLI version:
```bash
databricks --version
```

## 5. Authentication Token Access
```bash
az account get-access-token --resource=https://management.core.windows.net/ --query accessToken -o tsv > token.txt
```
This token is used for secure Key Vault-backed scope creation via REST.

## 6. Secrets Verification
```bash
databricks secrets list-scopes --profile <profile-name>
```

---


🛠 Databricks CLI Environment Setup Cheat Sheet

1. 🐍 Create Python Virtual Environment
   - python -m venv dbx_env
   - .\dbx_env\Scripts\activate  (Windows)
   - source dbx_env/bin/activate   (macOS/Linux)

2. 📦 Install Required Packages
   - pip install --upgrade pip
   - pip install git+https://github.com/databricks/databricks-cli.git

3. 🧪 Verify Installation
   - databricks --version

4. 🔐 Set Personal Access Token (PAT)
   - databricks configure --token
     Enter your Databricks host URL and token when prompted

5. 📁 Setup Project Folder
   - Create and structure: mkdir my_project && cd my_project
   - Include files like config.json, README.md, etc.

💡 Tips
   - Use virtual environments to isolate dependencies
   - Add `dbx_env/` to `.gitignore`
   - Use `requirements.txt` to track packages

