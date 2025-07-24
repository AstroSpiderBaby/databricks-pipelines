def read_sql_table(table_name: str, database: str = "fury161"):
    """
    Reads a SQL Server table into a Spark DataFrame.
    """
    from pyspark.sql import SparkSession
    import builtins

    dbutils = builtins.dbutils if hasattr(builtins, "dbutils") else None
    if dbutils is None:
        raise RuntimeError("dbutils not available in this environment.")

    spark = SparkSession.builder.getOrCreate()

    host = "100.100.211.77"
    port = 1433
    jdbc_url = f"jdbc:sqlserver://{host}:{port};database={database}"

    user = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-user")
    password = dbutils.secrets.get(scope="databricks-secrets-lv426", key="sql-password")

    connection_properties = {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
