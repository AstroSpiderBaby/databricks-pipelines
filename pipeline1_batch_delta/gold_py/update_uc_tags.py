u = spark.sql("SELECT current_user() AS u").collect()[0].u
t = spark.sql("SELECT current_timestamp() AS t").collect()[0].t
spark.sql(f"""
  ALTER TABLE thebetty.gold.final_vendor_summary
  SET TAGS (
    'last_run_user' = '{u}',
    'last_pipeline_run' = '{t}'
  )
""")
