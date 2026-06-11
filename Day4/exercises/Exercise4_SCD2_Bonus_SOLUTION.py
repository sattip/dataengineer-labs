# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 4/4 (Bonus): SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
DIM = "workspace.aade.dim_request_scd2"
spark.sql(f"DROP TABLE IF EXISTS {DIM}")

initial = spark.createDataFrame(
    [(1,"passed"),(2,"passed"),(3,"flagged"),(4,"passed"),(5,"rejected")],
    ["request_id","audit_outcome"]
).withColumn("valid_from", current_timestamp()) \
 .withColumn("valid_to", lit(None).cast("timestamp")) \
 .withColumn("is_current", lit(True))
initial.write.format("delta").mode("overwrite").saveAsTable(DIM)
print(f"✓ initial: {spark.table(DIM).count()}")

# COMMAND ----------

changes = spark.createDataFrame(
    [(2,"rejected"),(3,"passed"),(6,"passed")], ["request_id","audit_outcome"]
)

# COMMAND ----------

# TODO 1 — 2-part staged source
close_rows = changes.withColumn("mergeKey", col("request_id"))
existing_changed = (
    changes.alias("c")
    .join(spark.table(DIM).filter("is_current = true").alias("d"), "request_id")
    .where(col("c.audit_outcome") != col("d.audit_outcome"))
    .select("c.request_id", "c.audit_outcome")
    .withColumn("mergeKey", lit(None).cast("int"))
)
staged = close_rows.unionByName(existing_changed)
staged.createOrReplaceTempView("scd2_staged")
staged.orderBy("request_id").show()

# COMMAND ----------

# TODO 2 — SCD2 MERGE
spark.sql(f"""
    MERGE INTO {DIM} t
    USING scd2_staged s
    ON t.request_id = s.mergeKey AND t.is_current = true
    WHEN MATCHED AND t.audit_outcome <> s.audit_outcome THEN
        UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (request_id, audit_outcome, valid_from, valid_to, is_current)
        VALUES (s.request_id, s.audit_outcome, current_timestamp(), NULL, true)
""")
spark.table(DIM).orderBy("request_id","valid_from").show(truncate=False)

# COMMAND ----------

print("total:", spark.table(DIM).count(),
      "current:", spark.table(DIM).filter("is_current").count(),
      "history:", spark.table(DIM).filter("is_current = false").count())
print("✅ ΛΥΣΗ Μέρους 4 ολοκληρώθηκε — SCD2 complete")
