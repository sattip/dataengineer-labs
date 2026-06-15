# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ ΛΥΣΗ — Μέρος 2/4: Data Skew (Detection + Salting)

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, max as spark_max, avg, spark_partition_id
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
FACT = "workspace.aade.skew_fact"
N = 2_000_000; HOT_AFM = "100000000"; SALT_N = 16

fact = (spark.range(N)
    .withColumn("afm", when(col("id")%10 != 0, lit(HOT_AFM)).otherwise((col("id")%100000+200000000).cast("string")))
    .withColumn("amount_eur", (col("id")%1000+1).cast("double")))
fact.write.format("delta").mode("overwrite").saveAsTable(FACT)

# COMMAND ----------

# TODO 1 — detect
key_dist = spark.table(FACT).groupBy("afm").count()
top = key_dist.orderBy(col("count").desc()).first()
max_count = top["count"]; avg_count = key_dist.agg(avg("count")).collect()[0][0]
skew_ratio = max_count/avg_count
print(f"top={top['afm']} max={max_count:,} ratio={skew_ratio:,.0f}x")

# COMMAND ----------

# TODO 2 — partition imbalance (before)
skewed_rep = spark.table(FACT).repartition(col("afm"))
skew_max_part = skewed_rep.groupBy(spark_partition_id().alias("pid")).count().agg(spark_max("count")).collect()[0][0]
print("skew max partition:", f"{skew_max_part:,}")

# COMMAND ----------

# TODO 3 — salted two-stage aggregation
plain = spark.table(FACT).groupBy("afm").agg(spark_sum("amount_eur").alias("total"))
plain_hot = plain.filter(col("afm")==HOT_AFM).collect()[0]["total"]

salted = spark.table(FACT).withColumn("salt", (col("id")%SALT_N).cast("int"))
stage1 = salted.groupBy("afm","salt").agg(spark_sum("amount_eur").alias("partial"))
stage2 = stage1.groupBy("afm").agg(spark_sum("partial").alias("total"))
salted_hot = stage2.filter(col("afm")==HOT_AFM).collect()[0]["total"]
print("plain:", plain_hot, "salted:", salted_hot, "equal:", plain_hot==salted_hot)

# COMMAND ----------

# TODO 4 — partition balance (after)
salted_rep = salted.repartition(col("afm"), col("salt"))
salt_max_part = salted_rep.groupBy(spark_partition_id().alias("pid")).count().agg(spark_max("count")).collect()[0][0]
print(f"skew max={skew_max_part:,} salted max={salt_max_part:,} improvement={skew_max_part/salt_max_part:,.1f}x")

# COMMAND ----------

# AQE
for k in ["spark.sql.adaptive.enabled","spark.sql.adaptive.skewJoin.enabled"]:
    try:
        print(k, "=", spark.conf.get(k))
    except Exception:
        print(k, "= (μη-αναγνώσιμο σε serverless· ON by default)")

# COMMAND ----------

print("✅ ΛΥΣΗ Μέρους 2 ολοκληρώθηκε")
