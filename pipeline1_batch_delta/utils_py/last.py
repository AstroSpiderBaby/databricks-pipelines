# Copy Vendor Compliance file into Unity Catalog volume
dbutils.fs.cp(
    "dbfs:/mnt/external-ingest/Vendor_Compliance.csv",  # Adjust path if needed
    "dbfs:/Volumes/thebetty/bronze/landing_zone/Vendor_Compliance.csv"
)

# Optional: confirm it landed
display(dbutils.fs.ls("dbfs:/Volumes/thebetty/bronze/landing_zone/"))