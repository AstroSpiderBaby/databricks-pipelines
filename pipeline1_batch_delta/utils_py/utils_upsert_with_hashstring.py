from pyspark.sql.functions import col

def upsert_with_hashstring(df_new, path, primary_key, hash_col="hashstring"):
    from delta.tables import DeltaTable
    spark = df_new.sparkSession

    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)

        condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
        target_cols = set(delta_table.toDF().columns)
        source_cols = set(df_new.columns)
        common_cols = list(target_cols & source_cols)
        set_expr = {col: f"source.{col}" for col in common_cols}

        # ✅ Check if there are any differences
        diff_count = (
            delta_table.alias("target")
            .merge(
                df_new.alias("source"),
                condition
            )
            .whenMatchedUpdate(
                condition=f"target.{hash_col} != source.{hash_col}",
                set=set_expr
            )
            .whenNotMatchedInsert(
                values=set_expr
            )
            ._jbuilder.toDF()
            .count()
        )

        print(f"🔍 Records changed or inserted: {diff_count}")
        # Even if diff_count == 0, we don't fail
        return

    else:
        df_new.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(path)
