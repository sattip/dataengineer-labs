# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 5/6 (Advanced): Liquid Clustering & Data Skipping

# COMMAND ----------

import io, contextlib
from pyspark.sql.functions import col, count

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
SRC, CLUSTERED = "workspace.aade.lc_source", "workspace.aade.lc_clustered"

def get_plan(df):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode="formatted")
    return buf.getvalue()

N = 3_000_000
(spark.range(N)
    .withColumn("afm",(col("id")%100000+100000000).cast("string"))
    .withColumn("region_id",(col("id")%8).cast("int"))
    .withColumn("service_id",(col("id")%5).cast("int"))
    .withColumn("amount_eur",(col("id")%1000+1).cast("double"))
 .write.format("delta").mode("overwrite").saveAsTable(SRC))

# COMMAND ----------

# TODO 1 — liquid clustering
spark.sql(f"""
    CREATE OR REPLACE TABLE {CLUSTERED}
    CLUSTER BY (region_id, service_id)
    AS SELECT * FROM {SRC}
""")
detail = spark.sql(f"DESCRIBE DETAIL {CLUSTERED}").collect()[0]
print("clusteringColumns:", detail["clusteringColumns"], "numFiles:", detail["numFiles"])

# COMMAND ----------

# TODO 2 — OPTIMIZE
spark.sql(f"OPTIMIZE {CLUSTERED}")
opt_ran = spark.sql(f"DESCRIBE HISTORY {CLUSTERED}").filter("operation = 'OPTIMIZE'").count()
print("OPTIMIZE ops:", opt_ran)

# COMMAND ----------

# TODO 3 — filtered query / data skipping
q = spark.table(CLUSTERED).filter((col("region_id")==3) & (col("service_id")==2))
plan = get_plan(q)
print("skipping in plan:", ("PushedFilters" in plan) or ("DataFilters" in plan) or ("region_id" in plan))
print("rows:", q.count())

# COMMAND ----------

# TODO 4 — deletion vectors + delete
spark.sql(f"ALTER TABLE {CLUSTERED} SET TBLPROPERTIES (delta.enableDeletionVectors = true)")
before = spark.table(CLUSTERED).count()
spark.sql(f"DELETE FROM {CLUSTERED} WHERE service_id = 0")
after = spark.table(CLUSTERED).count()
dv_on = spark.sql(f"SHOW TBLPROPERTIES {CLUSTERED}").filter("key='delta.enableDeletionVectors'").collect()
print(f"before {before} after {after} DV {dv_on[0]['value'] if dv_on else 'n/a'}")

# COMMAND ----------

# TODO 5 — change clustering keys (no rewrite)
spark.sql(f"ALTER TABLE {CLUSTERED} CLUSTER BY (afm)")
new_detail = spark.sql(f"DESCRIBE DETAIL {CLUSTERED}").collect()[0]
print("new clusteringColumns:", new_detail["clusteringColumns"])

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 5 ολοκληρώθηκε")
