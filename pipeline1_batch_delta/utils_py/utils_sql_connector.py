"""
Utility Script: utils_sql_connector.py

Securely connects to a SQL Server instance using JDBC and reads tables into Spark DataFrames.
"""

from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.getOrCreate()

# Define secret scope and keys
secret_scope = "lv426"
jdbc_url = dbutils.secrets.get(scope=secret_scope, key="sql-jdbc-url")
user = dbutils.secrets.get(scope=secret_scope, key="sql-user")
password = dbutils.secrets.get(scope=secret_scope, key="sql-password")

connection_properties = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

def read_sql_table(table_name: str):
    """
    Reads a SQL Server table into a Spark DataFrame.

    Args:
        table_name (str): The name of the SQL Server table to read.

    Returns:
        DataFrame: Spark DataFrame containing the table data.
    """
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
