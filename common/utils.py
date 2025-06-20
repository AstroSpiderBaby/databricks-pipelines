# common/utils.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from typing import Optional

def read_csv(spark: SparkSession, path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame.
    """
    return spark.read.option("header", header).option("inferSchema", infer_schema).csv(path)

def read_json(spark: SparkSession, path: str, multiline: bool = False) -> DataFrame:
    """
    Reads a JSON file into a Spark DataFrame.
    """
    return spark.read.option("multiline", multiline).json(path)

def standardize_columns(df: DataFrame) -> DataFrame:
    """
    Lowercases and replaces spaces in column names.
    """
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower().replace(" ", "_"))
    return df

def write_delta(df: DataFrame, path: str, mode: str = "overwrite", partition_by: Optional[list] = None) -> None:
    """
    Writes a DataFrame to Delta format.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)
