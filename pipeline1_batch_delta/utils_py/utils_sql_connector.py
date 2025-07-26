"""
sql_connector.py

Securely retrieves a SQL Server table via JDBC using secrets stored in Azure Key Vault.
"""

from pyspark.sql import SparkSession
from typing import Optional

# === Ensure dbutils is available regardless of context ===
try:
    dbutils  # If available (e.g. notebooks), use as-is
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())

# === Get secrets from Azure Key Vault-backed scope ===
jdbc_url = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-jdbc-url")
sql_user = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-user")
sql_password = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-password")

# === JDBC connection properties ===
connection_properties = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def read_sql_table(table_name: str, spark: Optional[SparkSession] = None):
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
