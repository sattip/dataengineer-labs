# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ AADE DLT — Source Data Generator
# MAGIC
# MAGIC **Σκοπός:** Γράφει mock CSV files στο UC volume για να τα διαβάσει το DLT pipeline.
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/AADE_DLT_Generator.py
# MAGIC > ```
# MAGIC
# MAGIC **Τρέχει στο notebook UI** (όχι στο DLT pipeline). Κάθε φορά που το τρέχεις,
# MAGIC γράφει νέα batch αρχεία — το DLT pipeline θα τα πιάσει incrementally.

# COMMAND ----------

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logging.getLogger("pyspark.sql.connect.client.core").setLevel(logging.CRITICAL)
logging.getLogger("py4j").setLevel(logging.CRITICAL)
logging.getLogger("grpc").setLevel(logging.CRITICAL)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

volume_root = "/Volumes/workspace/aade/aade_data/streaming/raw"
sources = ["taxis", "mydata", "kep", "efka"]
for src in sources:
    os.makedirs(f"{volume_root}/{src}", exist_ok=True)

# Auto batch ID από timestamp
batch_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
np.random.seed(int(datetime.utcnow().timestamp()) % 10000)

afms = [f"{900000000 + i:09d}" for i in range(1, 21)]


def write_source(name, df):
    path = f"{volume_root}/{name}/batch_{batch_ts}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ {name:8s} → {len(df):4d} rows | {path.replace(volume_root, '/Volumes/.../raw')}")


# 1. TAXIS
taxis_data = []
for i in range(50):
    taxis_data.append({
        "statement_id": f"TX{batch_ts[-6:]}{i:03d}",
        "afm": np.random.choice(afms) if np.random.random() > 0.05 else None,
        "fiscal_year": 2025,
        "tax_category": np.random.choice(["IncomeTax", "VAT", "PropertyTax"]),
        "tax_base": round(np.random.uniform(5000, 80000), 2),
        "tax_amount": round(np.random.uniform(800, 18000), 2),
        "status": np.random.choice(["Submitted", "Approved", "Rejected"], p=[0.5, 0.4, 0.1]),
        "submitted_at": (datetime.utcnow() - timedelta(hours=np.random.randint(0, 48))).isoformat(),
    })
write_source("taxis", pd.DataFrame(taxis_data))

# 2. myDATA
mydata_data = []
for i in range(80):
    issuer = np.random.choice(afms)
    receiver = np.random.choice([a for a in afms if a != issuer])
    net = round(np.random.uniform(50, 5000), 2)
    vat = round(net * 0.24, 2)
    mydata_data.append({
        "invoice_id": f"INV{batch_ts[-6:]}{i:03d}",
        "issuer_afm": issuer,
        "receiver_afm": receiver,
        "invoice_date": (datetime.utcnow() - timedelta(days=np.random.randint(0, 30))).date().isoformat(),
        "net_amount": net,
        "vat_amount": vat,
        "total_amount": round(net + vat, 2),
        "transmission_status": np.random.choice(["Accepted", "Rejected", "Pending"], p=[0.85, 0.05, 0.10]),
    })
write_source("mydata", pd.DataFrame(mydata_data))

# 3. KEP events
kep_data = []
event_types = ["BirthCertificate", "TaxClearance", "ResidencePermit", "ConfirmationOfStudies", "FamilyStatus"]
for i in range(120):
    kep_data.append({
        "event_id": f"KEP{batch_ts[-6:]}{i:04d}",
        "afm": np.random.choice(afms),
        "event_type": np.random.choice(event_types),
        "kep_office": np.random.choice(["Athens-Center", "Thessaloniki", "Patras", "Heraklion", "Larissa"]),
        "event_ts": (datetime.utcnow() - timedelta(minutes=np.random.randint(0, 720))).isoformat(),
        "status": np.random.choice(["Completed", "InProgress", "Failed"], p=[0.85, 0.12, 0.03]),
        "duration_seconds": np.random.randint(120, 3600),
    })
write_source("kep", pd.DataFrame(kep_data))

# 4. EFKA contributions
efka_data = []
for i in range(60):
    efka_data.append({
        "contribution_id": f"EFK{batch_ts[-6:]}{i:03d}",
        "afm": np.random.choice(afms),
        "contribution_month": (datetime.utcnow() - timedelta(days=np.random.randint(0, 90))).strftime("%Y-%m"),
        "category": np.random.choice(["Employee", "SelfEmployed", "Pensioner"]),
        "gross_income": round(np.random.uniform(800, 6000), 2),
        "contribution_amount": round(np.random.uniform(120, 1200), 2),
        "payment_status": np.random.choice(["Paid", "Pending", "Overdue"], p=[0.80, 0.15, 0.05]),
    })
write_source("efka", pd.DataFrame(efka_data))

print(f"\n✓ Batch {batch_ts} written → 4 sources, {50+80+120+60} total rows")
print(f"\n📂 Volume contents:")
for src in sources:
    files = os.listdir(f"{volume_root}/{src}")
    print(f"  {src:8s}: {len(files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Επόμενα βήματα
# MAGIC
# MAGIC 1. ✅ Έχεις τα CSV files στο volume
# MAGIC 2. ➡️ Δημιούργησε το DLT pipeline στο Databricks UI:
# MAGIC    **Workflows → Pipelines → Create pipeline**
# MAGIC 3. ➡️ Διάλεξε ως source notebook το `AADE_DLT_Pipeline.py`
# MAGIC 4. ➡️ Run pipeline → δες το DAG με 12 tables
# MAGIC 5. 🔁 Re-run **αυτό** το generator όποτε θες νέα data — το DLT θα τα πιάσει incrementally
