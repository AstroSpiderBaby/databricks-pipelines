# utils_sql_connector.py
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def get_dbutils():
    return DBUtils(SparkSession.builder.getOrCreate())

def get_spark():
    return SparkSession.builder.getOrCreate()

def read_sql_table(table_name: str):
    spark = get_spark()
    dbutils = get_dbutils()

    secret_scope = "lv426"
    jdbc_url = dbutils.secrets.get(scope=secret_scope, key="sql-jdbc-url")
    user = dbutils.secrets.get(scope=secret_scope, key="sql-user")
    password = dbutils.secrets.get(scope=secret_scope, key="sql-password")

    connection_properties = {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
