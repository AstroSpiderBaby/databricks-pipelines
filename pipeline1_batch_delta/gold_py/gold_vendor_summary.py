from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, countDistinct

# Set up Spark session
spark = SparkSession.builder.appName("Gold Vendor Summary").getOrCreate()

# Load cleaned Silver
df_prep = spark.read.format("delta").load("/mnt/delta/silver/final_vendor_summary_prep")

# Load compliance and alias it to resolve ambiguity
df_compliance = (
    spark.read.format("delta")
    .load("/mnt/delta/silver/vendor_compliance_clean")
    .alias("compliance")
)

# Join and aggregate
df_gold = df_prep.join(df_compliance, on="vendor_id", how="left").groupBy(
    "vendor_id", "vendor_name"
).agg(
    countDistinct("invoice_id").alias("total_invoices"),
    max("due_date").alias("latest_due_date"),
    max("invoice_date").alias("latest_invoice_date"),
    max(col("compliance.last_audit_date")).alias("last_audit_date"),
    max(col("compliance.compliance_score")).alias("compliance_score"),
    max(col("compliance.status")).alias("compliance_status"),
)

# Write to Gold Delta Table
df_gold.write.format("delta").mode("overwrite").save("/mnt/delta/gold/vendor_summary")

# Optional: Show a preview
df_gold.show()
