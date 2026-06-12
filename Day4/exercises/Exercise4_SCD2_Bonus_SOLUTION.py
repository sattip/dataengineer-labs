# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 4/4 (Bonus): SCD Type 2

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
DIM = "workspace.aade.dim_request_scd2"
spark.sql(f"DROP TABLE IF EXISTS {DIM}")

initial = spark.createDataFrame(
    [(1,"passed",30),(2,"passed",40),(3,"flagged",50),(4,"passed",60),(5,"rejected",70),(6,"passed",80)],
    ["request_id","audit_outcome","wait_time_minutes"]
).withColumn("version", lit(1)) \
 .withColumn("valid_from", current_timestamp()) \
 .withColumn("valid_to", lit(None).cast("timestamp")) \
 .withColumn("is_current", lit(True))
initial.write.format("delta").mode("overwrite").saveAsTable(DIM)
print(f"✓ initial {spark.table(DIM).count()}")

# COMMAND ----------

# TODO 1 — apply_scd2
def apply_scd2(changes):
    current = spark.table(DIM).filter("is_current = true")
    close_rows = changes.withColumn("mergeKey", col("request_id")).withColumn("new_version", lit(1))
    changed = (
        changes.alias("c").join(current.alias("d"), "request_id")
        .where((col("c.audit_outcome") != col("d.audit_outcome")) |
               (col("c.wait_time_minutes") != col("d.wait_time_minutes")))
        .select("c.request_id","c.audit_outcome","c.wait_time_minutes",
                (col("d.version") + 1).alias("new_version"))
        .withColumn("mergeKey", lit(None).cast("int"))
    )
    staged = close_rows.unionByName(changed)
    staged.createOrReplaceTempView("scd2_staged")
    spark.sql(f"""
        MERGE INTO {DIM} t
        USING scd2_staged s
        ON t.request_id = s.mergeKey AND t.is_current = true
        WHEN MATCHED AND (t.audit_outcome <> s.audit_outcome
                          OR t.wait_time_minutes <> s.wait_time_minutes) THEN
            UPDATE SET t.is_current = false, t.valid_to = current_timestamp()
        WHEN NOT MATCHED THEN
            INSERT (request_id, audit_outcome, wait_time_minutes, version, valid_from, valid_to, is_current)
            VALUES (s.request_id, s.audit_outcome, s.wait_time_minutes, s.new_version,
                    current_timestamp(), NULL, true)
    """)

# COMMAND ----------

# TODO 2 — day 2
day2 = spark.createDataFrame([(2,"rejected",40),(3,"flagged",999),(7,"passed",25)],
                             ["request_id","audit_outcome","wait_time_minutes"])
apply_scd2(day2)
print(f"day2: total={spark.table(DIM).count()} current={spark.table(DIM).filter('is_current').count()}")  # 9, 7

# COMMAND ----------

# TODO 3 — day 3
day3 = spark.createDataFrame([(2,"rejected",500),(8,"flagged",15)],
                             ["request_id","audit_outcome","wait_time_minutes"])
apply_scd2(day3)
print(f"day3: total={spark.table(DIM).count()} current={spark.table(DIM).filter('is_current').count()}")  # 11, 8

# COMMAND ----------

# TODO 4 — history of id=2
(spark.table(DIM).filter(col("request_id") == 2).orderBy("version")
    .select("request_id","version","audit_outcome","wait_time_minutes","valid_to","is_current")
    .show(truncate=False))

# COMMAND ----------

print("total:", spark.table(DIM).count(), "current:", spark.table(DIM).filter("is_current").count(),
      "history:", spark.table(DIM).filter("is_current=false").count())
print("✅ ΛΥΣΗ Μέρους 4 ολοκληρώθηκε")
