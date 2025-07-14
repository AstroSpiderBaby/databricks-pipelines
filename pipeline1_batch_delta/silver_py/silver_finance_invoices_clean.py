from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper, trim, when, input_file_name, lit
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.getOrCreate()

# Load Bronze data
df = spark.read.format("delta").load("/mnt/delta/bronze/finance_invoices")

# Step 1: Clean columns
df_clean = (
    df.withColumn("vendor", upper(trim(col("vendor"))))
      .withColumn("invoice_date", to_date("invoice_date"))
      .withColumn("due_date", to_date("due_date"))
      .withColumn("paid_flag", when(col("paid") == "Yes", 1).otherwise(0).cast(IntegerType()))
      .drop("paid", "vendor_name", "rating", "location")
)

# Step 2: Vendor map
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

# Step 3: Join and keep metadata
df_final = df_clean.join(df_map, on="vendor", how="inner") \
    .select(
        "vendor_id", "invoice_id", "amount_usd", "invoice_date", "due_date",
        "paid_flag", "source_file", "ingestion_type"
    )

# Step 4: Write to Silver
output_path = "/mnt/delta/silver/finance_invoices_v2"

df_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("invoice_date") \
    .save(output_path)
