# Databricks notebook source
# MAGIC %md
# MAGIC # 🏆 Day 6 — Capstone Mini-Project
# MAGIC
# MAGIC **Duration:** 20 minutes implementation + 15 minutes presentations (Hour 3)
# MAGIC **Mode:** No hints, no starter code — **εσείς σχεδιάζετε και υλοποιείτε**
# MAGIC **Prerequisites:** Ολοκληρώσατε Blocks A + B
# MAGIC
# MAGIC ## 🎯 Στόχος
# MAGIC > Συνδυάστε όλα όσα μάθατε σε ένα **end-to-end ΑΑΔΕ pipeline** από scratch.
# MAGIC > Καμία βοήθεια — αναφέρετε στο reading material αν χρειαστεί.
# MAGIC > Στο τέλος, **παρουσιάζετε** τα results στον trainer (5 min ανά person).
# MAGIC
# MAGIC ## 📋 Project Brief
# MAGIC
# MAGIC Είστε νέοι Data Engineers της ΑΑΔΕ. Το προϊστάμενο γραφείο σας ζητάει:
# MAGIC > «Φτιάξτε pipeline που τραβάει 2 source streams (TAXIS + myDATA), τα ενώνει,
# MAGIC > εντοπίζει **suspect cases** (φορολογούμενοι που δηλώνουν χαμηλό εισόδημα αλλά
# MAGIC > έχουν υψηλή invoice activity), και τοποθετεί τα top-100 σε ένα Gold table
# MAGIC > για audit prioritization. Το pipeline πρέπει να τρέχει daily και να έχει
# MAGIC > full lineage + data quality monitoring.»
# MAGIC
# MAGIC ## ✅ Acceptance Criteria
# MAGIC Στο τέλος των 60 λεπτών πρέπει να έχετε:
# MAGIC
# MAGIC 1. **2 Bronze streaming tables** (Auto Loader από CSV files)
# MAGIC 2. **2 Silver tables** με data quality + MERGE
# MAGIC 3. **1 Gold table** `audit_priority_top100` με τους ύποπτους
# MAGIC 4. **1 DQ summary table** με per-rule failure metrics
# MAGIC 5. **Incremental run capability** — δεύτερη εκτέλεση πιάνει μόνο τα νέα data
# MAGIC 6. **Documentation** — markdown cells που εξηγούν τη λογική
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day6/Capstone_3h_Workshop/03_Capstone_MiniProject.py
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧰 Provided Setup (μην αλλάξετε τίποτα εδώ)

# COMMAND ----------

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import col

for n in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
          "pyspark", "py4j", "grpc"):
    logging.getLogger(n).setLevel(logging.CRITICAL)

YOUR_NAME = "george"  # ← change me
SCHEMA = f"workspace.capstone_{YOUR_NAME}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SCHEMA}.raw")

VOLUME = f"/Volumes/workspace/capstone_{YOUR_NAME}/raw"
TAXIS_SRC = f"{VOLUME}/taxis"
MYDATA_SRC = f"{VOLUME}/mydata"
CHKPT = f"{VOLUME}/checkpoints"

for p in [TAXIS_SRC, MYDATA_SRC, CHKPT]:
    os.makedirs(p, exist_ok=True)

# Generate mock data — Batch 1
np.random.seed(hash(YOUR_NAME) % 1000)
afms = [f"{900000000 + i:09d}" for i in range(50)]

# TAXIS — χαμηλό εισόδημα για κάποιους που έχουν πολλά τιμολόγια (suspect)
suspect_afms = afms[:10]  # οι 10 πρώτοι θα είναι «ύποπτοι»

def gen_taxis_batch(batch_id: int):
    rows = []
    for i, afm in enumerate(afms):
        # suspects δηλώνουν χαμηλά
        income = np.random.uniform(2000, 8000) if afm in suspect_afms else np.random.uniform(15000, 60000)
        rows.append({
            "statement_id": f"TX{batch_id:02d}{i:04d}",
            "afm": afm,
            "fiscal_year": 2025,
            "region": np.random.choice(["Αττική", "Θεσσαλονίκη", "Κρήτη"]),
            "declared_income": round(income, 2),
            "tax_paid": round(income * 0.15, 2),
            "status": "APPROVED",
            "submitted_at": datetime.utcnow().isoformat(),
        })
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    pd.DataFrame(rows).to_csv(f"{TAXIS_SRC}/taxis_b{batch_id:02d}_{ts}.csv", index=False)
    return len(rows)

# myDATA — suspects εκδίδουν πολλά τιμολόγια (μεγάλη πραγματική δραστηριότητα)
def gen_mydata_batch(batch_id: int):
    rows = []
    for i in range(150):
        issuer = np.random.choice(afms)
        # suspects έχουν 5× περισσότερα invoices
        if issuer in suspect_afms:
            multiplier = 5
        else:
            multiplier = 1
        for _ in range(multiplier):
            amount = round(np.random.uniform(500, 8000), 2)
            rows.append({
                "invoice_id": f"INV{batch_id:02d}{len(rows):05d}",
                "issuer_afm": issuer,
                "receiver_afm": np.random.choice([a for a in afms if a != issuer]),
                "invoice_date": (datetime.utcnow() - timedelta(days=np.random.randint(0, 30))).date().isoformat(),
                "net_amount": amount,
                "vat_amount": round(amount * 0.24, 2),
                "total_amount": round(amount * 1.24, 2),
                "status": np.random.choice(["Accepted", "Rejected"], p=[0.95, 0.05]),
            })
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    pd.DataFrame(rows).to_csv(f"{MYDATA_SRC}/mydata_b{batch_id:02d}_{ts}.csv", index=False)
    return len(rows)

taxis_b1 = gen_taxis_batch(1)
mydata_b1 = gen_mydata_batch(1)
print(f"✅ Setup ready in {SCHEMA}")
print(f"   TAXIS  source: {TAXIS_SRC}  ({taxis_b1} rows)")
print(f"   myDATA source: {MYDATA_SRC} ({mydata_b1} rows)")
print(f"   Checkpoints:   {CHKPT}")
print(f"\n   💡 Hint: οι suspect ΑΦΜ είναι: {suspect_afms[:5]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 START HERE — Capstone Implementation
# MAGIC
# MAGIC ## Step 1 — Bronze layer (15')
# MAGIC
# MAGIC Φτιάξτε 2 streaming Bronze tables με Auto Loader:
# MAGIC - `{SCHEMA}.bronze_taxis` ← από `{TAXIS_SRC}/`
# MAGIC - `{SCHEMA}.bronze_mydata` ← από `{MYDATA_SRC}/`
# MAGIC
# MAGIC Πρόσθεστε metadata columns (`_ingested_at`, `_source_file`).

# COMMAND ----------

# Step 1 — Your code here:




# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Silver layer με data quality (15')
# MAGIC
# MAGIC Φτιάξτε 2 Silver tables με data quality checks. Τι κρατάμε:
# MAGIC
# MAGIC **silver_taxis_clean:**
# MAGIC - afm: not null, 9 ψηφία
# MAGIC - declared_income: > 0
# MAGIC - tax_paid: >= 0
# MAGIC - tax_paid / declared_income μεταξύ 0 και 0.5 (sanity)
# MAGIC
# MAGIC **silver_mydata_clean:**
# MAGIC - issuer_afm, receiver_afm: not null, 9 ψηφία
# MAGIC - issuer_afm != receiver_afm
# MAGIC - total_amount > 0
# MAGIC - status = 'Accepted'
# MAGIC
# MAGIC **Bonus**: log DQ metrics σε ξεχωριστό πίνακα `{SCHEMA}.dq_summary`.

# COMMAND ----------

# Step 2 — Your code here:




# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Gold: Audit Priority Top-100 (20')
# MAGIC
# MAGIC Φτιάξτε `{SCHEMA}.audit_priority_top100` με τους 100 ύποπτους φορολογούμενους.
# MAGIC
# MAGIC ### Suspect formula
# MAGIC Υψηλή τιμή σημαίνει υψηλό audit priority:
# MAGIC ```
# MAGIC suspect_score = total_invoice_amount / NULLIF(declared_income, 0)
# MAGIC ```
# MAGIC (Πχ. ένας με 1000€ income και 50.000€ invoices έχει score 50 → πολύ ύποπτο)
# MAGIC
# MAGIC ### Required columns στο Gold:
# MAGIC | Column | Source |
# MAGIC |---|---|
# MAGIC | `afm` | join key |
# MAGIC | `region` | from silver_taxis |
# MAGIC | `declared_income` | from silver_taxis |
# MAGIC | `total_invoices_count` | from silver_mydata aggregation |
# MAGIC | `total_invoiced_amount` | from silver_mydata aggregation |
# MAGIC | `suspect_score` | calculated |
# MAGIC | `audit_rank` | row_number() ORDER BY suspect_score DESC |
# MAGIC | `computed_at` | current_timestamp |
# MAGIC
# MAGIC Φιλτράρετε `audit_rank <= 100`.

# COMMAND ----------

# Step 3 — Your code here:




# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Incremental Run Test (5')
# MAGIC
# MAGIC Παράξτε νέα data + ξανατρέξτε όλο το pipeline σας. Πρέπει να γίνει incremental:
# MAGIC bronze rows αυξάνονται, silver/gold ανανεώνονται με τα νέα.

# COMMAND ----------

# Generate Batch 2 (μην το αλλάξετε)
taxis_b2 = gen_taxis_batch(2)
mydata_b2 = gen_mydata_batch(2)
print(f"✅ Batch 2: TAXIS={taxis_b2}, myDATA={mydata_b2}")

# Step 4 — Re-run pipeline cells (Step 1-3) και verify ότι rows αυξάνονται




# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Documentation & Final Report (5')
# MAGIC
# MAGIC Συμπληρώστε το παρακάτω report cell.

# COMMAND ----------

# Final reporting (verification — αυτό μπορείτε να το αλλάξετε)
print("=" * 70)
print(f"           CAPSTONE FINAL REPORT — {SCHEMA}")
print("=" * 70)

try:
    layers = {
        "bronze_taxis":               "Bronze",
        "bronze_mydata":              "Bronze",
        "silver_taxis_clean":         "Silver",
        "silver_mydata_clean":        "Silver",
        "audit_priority_top100":      "Gold",
        "dq_summary":                 "Monitoring",
    }
    for table, layer in layers.items():
        try:
            cnt = spark.table(f"{SCHEMA}.{table}").count()
            print(f"  [{layer:10s}] {table:30s}: {cnt:6d} rows  ✅")
        except Exception:
            print(f"  [{layer:10s}] {table:30s}: NOT FOUND   ❌")

    # Top 5 suspects
    print("\n" + "=" * 70)
    print("           TOP 5 SUSPECTS (audit_rank 1-5)")
    print("=" * 70)
    spark.sql(f"""
        SELECT afm, region, declared_income,
               total_invoiced_amount, suspect_score, audit_rank
        FROM {SCHEMA}.audit_priority_top100
        ORDER BY audit_rank ASC
        LIMIT 5
    """).show(truncate=False)
except Exception as e:
    print(f"Error στο final report: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Self-Assessment Questions
# MAGIC
# MAGIC Πριν παρουσιάσετε στον trainer, σκεφτείτε:
# MAGIC
# MAGIC 1. **Acceptance criteria coverage:**
# MAGIC    - ☐ 2 Bronze streaming tables
# MAGIC    - ☐ 2 Silver με DQ
# MAGIC    - ☐ 1 Gold με top-100
# MAGIC    - ☐ 1 DQ summary
# MAGIC    - ☐ Incremental run δούλεψε
# MAGIC    - ☐ Markdown documentation
# MAGIC
# MAGIC 2. **Design choices:**
# MAGIC    - Γιατί διάλεξα incremental processing μέσω Auto Loader αντί για batch?
# MAGIC    - Πώς ορίζω «suspect»; Είναι το threshold μου δίκαιο;
# MAGIC    - Τι θα γινόταν αν το income ήταν 0;
# MAGIC
# MAGIC 3. **What could break in production:**
# MAGIC    - GDPR/PII concerns;
# MAGIC    - Late-arriving data;
# MAGIC    - Schema changes σε source;
# MAGIC    - Cost optimization;
# MAGIC
# MAGIC ## 🎤 Παρουσίαση στον trainer (5 min)
# MAGIC
# MAGIC Δείξτε:
# MAGIC 1. Catalog Explorer → schema σας με όλα τα tables
# MAGIC 2. Lineage tab σε ένα Gold table
# MAGIC 3. Top 5 suspects + explain γιατί score makes sense
# MAGIC 4. DESCRIBE HISTORY σε ένα Silver — δείξτε τα MERGE operations
# MAGIC 5. Run το pipeline μία επιπλέον φορά live και δείξτε ότι είναι incremental
# MAGIC
# MAGIC ## 🏁 Wrap-up
# MAGIC
# MAGIC Συγχαρητήρια! Έχετε χτίσει ένα **πλήρες ΑΑΔΕ pipeline** χωρίς starter code.
# MAGIC Αυτή η εμπειρία είναι ό,τι πιο κοντινό σε junior DE production work.
# MAGIC
# MAGIC ### Δείτε επίσης:
# MAGIC - `04_Solutions.py` — η δική μου implementation (μετά την παρουσίαση)
# MAGIC - `AADE_DLT_Pipeline.py` — η declarative version του ίδιου pipeline
