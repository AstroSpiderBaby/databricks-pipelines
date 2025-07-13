"""
silver_finance_invoices_clean.py

Cleans and enriches raw finance invoice data from Bronze and prepares it for Silver.
"""

import sys
sys.path.append("/Workspace/Repos/brucejenks@live.com/databricks-pipelines/pipeline1_batch_delta")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper, trim, when
from pyspark.sql.types import IntegerType

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Input and output paths
input_path = "/mnt/delta/bronze/bronze_finance_invoices"
output_path = "/mnt/delta/silver/silver_finance_invoices_clean"

# Step 1: Load Bronze data
df = spark.read.format("delta").load(input_path)

# Step 2: Clean fields
df_clean = (
    df.withColumn("vendor", upper(trim(col("vendor"))))
      .withColumn("invoice_date", to_date(col("invoice_date")))
      .withColumn("due_date", to_date(col("due_date")))
      .withColumn("paid_flag", when(col("paid") == "Yes", 1).otherwise(0).cast(IntegerType()))
      .drop("paid", "vendor_name", "rating", "location")
)

# Step 3: Vendor mapping
vendor_map = [
    ("WOLFE LLC", "V010"),
    ("MOORE-BERNARD", "V008"),
    ("GARCIA-JAMES", "V006"),
    ("ABBOTT-MUNOZ", "V001"),
    ("BLAIR PLC", "V003"),
    ("DUDLEY GROUP", "V004"),
    ("ARNOLD LTD", "V002"),
    ("MCCLURE, WARD AND LEE", "V007"),
    ("WILLIAMS AND SONS", "V009"),
    ("GALLOWAY-WYATT", "V005"),
]

df_map = spark.createDataFrame(vendor_map, ["vendor", "vendor_id"])

# Step 4: Join
df_final = df_clean.join(df_map, on="vendor", how="inner")

# Step 5: Select + reorder columns
df_final = df_final.select(
    "vendor_id", "invoice_id", "amount_usd", "invoice_date", "due_date",
    "paid_flag", "source_file", "ingestion_type"
)

# Step 6: Write to Silver
df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("invoice_date") \
    .save(output_path)
