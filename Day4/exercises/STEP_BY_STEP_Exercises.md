# 📋 Step-by-Step — Άσκηση Ημέρα 4 (Full vs Incremental)

**Οδηγός διευκόλυνσης** + **hints ανά TODO**. Φιλοσοφία: **🧠 ΕΝΝΟΙΑ → ✍️ TODO → self-check**.

> 4 notebooks · **~4.5 ώρες** (~58 TODOs συνολικά). Μαζί με την Ημέρα 3 (~3h) = **6ωρο+ Day 3+4**.

---

## 🚀 SETUP (~2')

- [x] Databricks (Free Edition + **Serverless** — Auto Loader & streaming ✅)
- [x] Import τα `Exercise*_STARTER.py`· τρέξτε με τη σειρά 1→2→3→4
- [x] Cell 0 κατεβάζει το `kep_requests.csv`
- [x] **Streaming cells:** αφήστε τα να ολοκληρωθούν (`awaitTermination`)

---

# 🔵 ΜΕΡΟΣ 1 — Full vs Incremental (~80', ~16 TODOs)

| TODO | Hint |
|---|---|
| 1a/1b | audit log: `delta` · `append` |
| 2a–2d | `"FULL"` · `current_timestamp` · `overwrite` · `==` |
| 3a–3d | `last_id` · `>` · `append` · (advance watermark έτοιμο) |
| 4a–4c | `=` · `MATCHED` · `NOT MATCHED` |
| 5 | `load_type` |

**✅ Expected:** full=10k, incremental=2k· audit log ≥3 runs· reconcile incr==full==10k· ~80% λιγότερη δουλειά.

> 🧑‍🏫 **Tip:** Το audit log είναι το «γιατί» — δείξτε τη σύγκριση rows_processed full vs incremental.

---

# 🟡 ΜΕΡΟΣ 2 — Auto Loader (~80', ~15 TODOs)

| TODO | Hint |
|---|---|
| 1a–1f | `cloudFiles` · `csv` · `SCHEMA_LOC` · `rescue` · `_metadata.file_path` · `current_timestamp` |
| 2a/2b | `CKPT` · `availableNow` |
| 3a | `run_autoloader()` |
| 4 | `isNotNull()` |
| 5a | `service_type` |

**✅ Expected:** 6k→10k (+4k)· batch3 → 10.100 με **rescued data** (schema drift)· Silver 5 service types.

> 🧑‍🏫 **Tip:** schema drift = νέα στήλη. Με `schemaEvolutionMode="rescue"` το pipeline ΔΕΝ σπάει —
> η νέα στήλη πάει στο `_rescued_data`. Δείξτε το περιεχόμενό του.

---

# 🟢 ΜΕΡΟΣ 3 — Streaming + foreachBatch (~75', ~15 TODOs)

| TODO | Hint |
|---|---|
| 1 | `delta` (streaming read) |
| 2a | `isin` (DQ: έγκυρα audit_outcome) |
| 2b | `append` (quarantine write) |
| 2c/2d | `request_id` · `desc` (Window dedup) |
| 2e | `1` (rn==1) |
| 2f/2g | `MATCHED` · `NOT MATCHED` (MERGE) |
| 2h | `service_type` (Gold groupBy) |
| 2i | `"flagged"` (conditional count) |
| 2j | `overwrite` (Gold write) |
| 3a/3b | `process_batch` · `availableNow` (Run 1) |
| 4a/4b/4c | `delta` · `process_batch` · `CKPT` (Run 2) |
| 5 | `CKPT` (Run 3 — ίδιο → idempotent) |

**✅ Expected:** run1=10k, run2=10.001, run3=10.001 (idempotent)· κακή id=10002 → **quarantine** (όχι Silver)·
dedup κρατά νέα id10001· Gold=5 με `pct_flagged`· 0 dupes.

> 🧑‍🏫 **Tip:** ο επεξεργαστής batch κάνει **4** πράγματα: **DQ split → dedup → MERGE → Gold+metrics**.
> Δείξε ότι η «κακή» γραμμή πάει quarantine (τίποτα δεν χάνεται σιωπηλά). Run 3 (no data) = exactly-once.

---

# 🏆 ΜΕΡΟΣ 4 — SCD Type 2 (~60', ~12 TODOs)

| TODO | Hint |
|---|---|
| 1a | `!=` (δεύτερο tracked πεδίο) |
| 1b | `1` (version + 1) |
| 1c | `None` (mergeKey) |
| 1d | `true` (is_current) |
| 1e | `<>` (MATCHED OR condition) |
| 1f | `false` (close old) |
| 1g | `true` (new current) |
| 2/3 | `apply_scd2(day2)` · `apply_scd2(day3)` |
| 4 | `2` |

**✅ Expected:** total 11· current 8· history 3· id2 → 3 versions· id3 (wait-only) → 2 versions.

> 🧑‍🏫 **Tip:** Whiteboard τις 3 versions του id2 με valid_from/valid_to. Τονίστε το multi-attribute
> detection (OR) — γι' αυτό το id3 (μόνο wait άλλαξε) πιάστηκε.

---

## 🧹 Reset

```python
for t in ["kep_requests_src","kep_bronze_full","kep_bronze_incr","kep_watermark","etl_audit_log",
          "kep_bronze_autoloader","kep_silver_by_service","kep_stream_src","kep_silver_stream",
          "kep_stream_quarantine","kep_gold_service_live","kep_stream_batchlog","dim_request_scd2"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
for p in ["kep_landing","_schemas/kep_autoloader","_checkpoints/kep_autoloader","_checkpoints/kep_silver_stream"]:
    dbutils.fs.rm(f"/Volumes/workspace/aade/aade_data/{p}", recurse=True)
```

## 🎯 Learning outcomes

full vs incremental· **audit log/metrics**· high-water-mark· append vs MERGE upsert· **Auto Loader**
(cloudFiles/checkpoint/rescued data/schema drift)· **Structured Streaming** (foreachBatch: DQ/quarantine+
dedup+MERGE+Gold+exactly-once)· **SCD Type 2** (versioned history). → Όλη η «Ημέρα 4: Incremental Ingestion & Streaming».
