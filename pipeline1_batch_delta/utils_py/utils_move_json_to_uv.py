dbutils.fs.cp(
    "dbfs:/mnt/external-ingest/web_form_submissions.json",
    "dbfs:/Volumes/thebetty/bronze/landing_zone/web_form_submissions.json"
)



display(dbutils.fs.ls("dbfs:/Volumes/thebetty/bronze/landing_zone/"))