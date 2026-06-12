# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 1/4: Partitioning, Joins & Caching

# COMMAND ----------

import time
from pyspark.sql.functions import col, broadcast, count, sum as spark_sum, avg, current_timestamp, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
FACT, DIM, PARTED, PERFLOG = ("workspace.aade.perf_requests_fact","workspace.aade.perf_regions_dim",
    "workspace.aade.perf_requests_partitioned","workspace.aade.perf_log")

N = 2_000_000
fact = (spark.range(N)
    .withColumn("afm",(col("id")%100000+100000000).cast("string"))
    .withColumn("region_id",(col("id")%8).cast("int"))
    .withColumn("service_id",(col("id")%5).cast("int"))
    .withColumn("amount_eur",(col("id")%1000+1).cast("double")))
fact.write.format("delta").mode("overwrite").saveAsTable(FACT)
regions = spark.createDataFrame(list(enumerate(
    ["Αττική","Κεντρική Μακεδονία","Θεσσαλία","Δυτική Ελλάδα","Κρήτη","Ιόνια Νησιά","Πελοπόννησος","Ήπειρος"])),
    ["region_id","region_name"])
regions.write.format("delta").mode("overwrite").saveAsTable(DIM)
spark.sql(f"CREATE TABLE IF NOT EXISTS {PERFLOG} (step STRING, n_partitions INT, rows BIGINT, duration_ms BIGINT, logged_at TIMESTAMP) USING delta")

def timed(step, fn):
    t0=time.time(); r=fn(); ms=int((time.time()-t0)*1000); print(f"  ⏱️ {step}: {ms} ms"); return r, ms

# COMMAND ----------

# TODO 1 — repartition / coalesce
base = spark.table(FACT)
rep = base.repartition(16); n_rep = rep.rdd.getNumPartitions()
col4 = rep.coalesce(4);     n_col = col4.rdd.getNumPartitions()
print(n_rep, n_col)

# COMMAND ----------

# TODO 2 — broadcast join
dim = spark.table(DIM)
joined = spark.table(FACT).join(broadcast(dim), on="region_id", how="left")
plan = joined._jdf.queryExecution().executedPlan().toString()
has_broadcast = "BroadcastHashJoin" in plan
print("BroadcastHashJoin:", has_broadcast)

# COMMAND ----------

# TODO 3 — cache
agg = joined.groupBy("region_name").agg(count("*").alias("n"), spark_sum("amount_eur").alias("total"))
agg.cache()
_, ms1 = timed("cold", lambda: agg.count())
_, ms2 = timed("cached", lambda: agg.orderBy(col("total").desc()).collect())
print("is_cached:", agg.is_cached)
agg.show(truncate=False)

# COMMAND ----------

# TODO 4 — partitioned write
joined.write.format("delta").mode("overwrite").partitionBy("region_name").saveAsTable(PARTED)
detail = spark.sql(f"DESCRIBE DETAIL {PARTED}").collect()[0]
print("partitionColumns:", detail["partitionColumns"])
one = spark.table(PARTED).filter(col("region_name")=="Αττική")
print("Αττική rows:", one.count())

# COMMAND ----------

# TODO 5 — perf log
(spark.createDataFrame(
    [("repartition16",n_rep,spark.table(FACT).count(),0),
     ("coalesce4",n_col,spark.table(FACT).count(),0),
     ("agg_cold",None,agg.count(),ms1),
     ("agg_cached",None,agg.count(),ms2)],
    ["step","n_partitions","rows","duration_ms"])
 .withColumn("logged_at", current_timestamp())
 .write.format("delta").mode("append").saveAsTable(PERFLOG))
spark.table(PERFLOG).orderBy("logged_at").show(truncate=False)

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 1 ολοκληρώθηκε")
