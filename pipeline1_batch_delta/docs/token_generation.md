🔐 Azure AAD Token Generation for Databricks (Azure Key Vault Integration)
This guide outlines how to generate an AAD token using the Azure CLI so you can use it to create a Databricks secret scope backed by Azure Key Vault.

✅ Step-by-Step
Open Command Prompt (not Git Bash for this step — Git Bash may misinterpret the redirect):

az account get-access-token --resource=https://management.core.windows.net/ --query accessToken -o tsv > token.txt
Navigate to your working directory where token.txt is saved:

cd /c/Users/<YourUsername>/path/to/scripts
Verify the token file was generated using Git Bash or VS Code terminal:

cat token.txt
If you see a long JWT-style token, you're good (starts with eyJ...).

⚠️ Common Issues
Running in Git Bash may cause issues with redirection. Always run the command in Command Prompt or PowerShell when saving output to a file.

If token.txt looks empty, re-run in cmd.exe.

Git Bash can still be used after token is created (e.g., to run curl with the token).

🔐 Using the Token
You can use the token to create the secret scope via curl:

curl -X POST https://adb-<your-id>.azuredatabricks.net/api/2.0/secrets/scopes/create \
-H "Authorization: Bearer $(cat token.txt)" \
-H "Content-Type: application/json" \
-d '{
  "scope": "lv426_kv",
  "scope_backend_type": "AZURE_KEYVAULT",
  "backend_azure_keyvault": {
    "resource_id": "/subscriptions/XXXX/resourceGroups/YYYY/providers/Microsoft.KeyVault/vaults/your-keyvault",
    "dns_name": "https://your-keyvault.vault.azure.net/"
  }
}'
🧼 Cleanup & Security
The token expires after a short time (~1 hour), so re-generate if needed.

Do not commit token.txt to source control.

Delete after use if possible:

rm token.txt