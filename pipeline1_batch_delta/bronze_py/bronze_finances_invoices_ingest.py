from pyspark.sql.functions import input_file_name, lit

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Define paths
input_path = "/mnt/raw-ingest/finance_invoice_data.csv"
output_path = "/mnt/delta/bronze/bronze_finance_invoices"

# Load CSV and add metadata columns
df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_type", lit("finance_invoices"))
)

# Write to Bronze
write_to_delta(
    df=df,
    path=output_path,
    partition_by=None,
    mode="overwrite",
    merge_schema=True,
    register_table=True,
    dry_run=False,
    verbose=True
)
