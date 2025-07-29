%md



# 🖥️ Local SQL Server Setup (SSMS)

This document outlines how to configure SQL Server locally (via SSMS) for secure integration with Databricks using JDBC and Azure Key Vault.

---

## ✅ Step-by-Step Configuration

| Step | Description |
|------|-------------|
| ✅ Enable Mixed Authentication | Open **Server Properties** > **Security** → Select `SQL Server and Windows Authentication mode`. |
| ➕ Create Login | In a new query window:<br>`CREATE LOGIN databricks_user WITH PASSWORD = 'yourStrongPassword';` |
| ➕ Create Database User | In your target database:<br>`CREATE USER databricks_user FOR LOGIN databricks_user;` |
| 👓 Grant Access | Read access:<br>`EXEC sp_addrolemember 'db_datareader', 'databricks_user';`<br>Optional write access:<br>`EXEC sp_addrolemember 'db_datawriter', 'databricks_user';` |
| 🔒 Add Secrets to Azure Key Vault | Add the following secrets to your Key Vault:<br>- `sql-user` = `databricks_user`<br>- `sql-password` = `yourStrongPassword`<br>Ensure secrets are **Enabled**. |
| 🧪 Test JDBC from Databricks | Use the helper script (`sql_connector.py`) in `/utils` to establish a secure JDBC connection. |

---
## 🔐 Secrets Scope and Key Vault Notes

* Secret scope used: `databricks-secrets-lv426`
* Keys expected:

  * `sql-user`
  * `sql-password`

---

## 🔧 Sample Connection Code

```python
jdbc_hostname = "<your-host>"
jdbc_port = 1433
jdbc_database = "fury161"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

username = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-user")
password = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-password")

properties = {
    "user": username,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read test table
df = spark.read.jdbc(url=jdbc_url, table="dbo.YourTable", properties=properties)
df.show()
```

---

## 🔒 Notes

* Confirm your firewall or local networking allows incoming connections from Databricks if required.
* Adjust the JDBC hostname to point to the local IP or machine name if accessible within the same network.
* You can optionally use Azure Data Factory or self-hosted integration runtime in future integrations.

---

> Last updated: July 2025
## 🔐 Notes

- This process uses **Azure Key Vault-backed secret scope** for secure credential management.
- The JDBC driver required (`com.microsoft.sqlserver.jdbc.SQLServerDriver`) is available by default in most Databricks runtimes.
- If needed, add IP firewall exceptions on your local machine to allow Databricks IPs to connect.

%md
# 🔤 Local SQL Server Setup (SSMS)

This document outlines how to configure SQL Server locally (via SSMS) for secure integration with Databricks using JDBC and Azure Key Vault.

---

## ✅ Step-by-Step Configuration

| Step                              | Description                                                                                                                                                          |
| --------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ✅ Enable Mixed Authentication     | Open **Server Properties** > **Security** → Select `SQL Server and Windows Authentication mode`.                                                                     |
| ➕ Create Login                    | In a new query window:<br>`CREATE LOGIN databricks_user WITH PASSWORD = 'yourStrongPassword';`                                                                       |
| ➕ Create Database User            | In your target database:<br>`CREATE USER databricks_user FOR LOGIN databricks_user;`                                                                                 |
| 👃 Grant Access                   | Read access:<br>`EXEC sp_addrolemember 'db_datareader', 'databricks_user';`<br>Optional write access:<br>`EXEC sp_addrolemember 'db_datawriter', 'databricks_user';` |
| 🔒 Add Secrets to Azure Key Vault | Add the following secrets to your Key Vault:<br>- `sql-user` = `databricks_user`<br>- `sql-password` = `yourStrongPassword`<br>Ensure secrets are **Enabled**.       |
| 🧪 Test JDBC from Databricks      | Use the helper script (`sql_connector.py`) in `/utils` to establish a secure JDBC connection.                                                                        |

---




