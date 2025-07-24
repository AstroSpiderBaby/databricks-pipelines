# Copy the file from old mount to Unity Catalog volume
dbutils.fs.cp(
    "dbfs:/mnt/raw-ingest/finance_invoice_data.csv",
    "dbfs:/Volumes/thebetty/bronze/landing_zone/Finances_Invoices.csv"
)



display(dbutils.fs.ls("dbfs:/Volumes/thebetty/bronze/landing_zone/"))