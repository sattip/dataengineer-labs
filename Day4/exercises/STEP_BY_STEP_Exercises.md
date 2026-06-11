# 📋 Step-by-Step — Άσκηση Ημέρα 4 (Full vs Incremental)

**Οδηγός διευκόλυνσης** + **hints ανά TODO**. Φιλοσοφία: **🧠 ΕΝΝΟΙΑ → ✍️ TODO → self-check**.

> 4 notebooks · ~4 ώρες. Μαζί με την Ημέρα 3 (~3h) = **6ωρο Day 3+4** με νήμα «incremental».

---

## 🚀 SETUP (~2')

- [x] Databricks (Free Edition + **Serverless** — υποστηρίζει Auto Loader & streaming ✅)
- [x] Import τα `Exercise*_STARTER.py`· τρέξτε με τη σειρά 1→2→3→4
- [x] Cell 0 κατεβάζει το `kep_requests.csv`
- [x] **Streaming cells:** αφήστε τα να ολοκληρωθούν (`awaitTermination` — τελειώνουν μόνα τους)

---

# 🔵 ΜΕΡΟΣ 1 — Full vs Incremental (~75')

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1 | Full load mode | `overwrite` |
| 2a | Incremental filter | `>` (request_id > last_id) |
| 2b | Append mode | `append` |
| 3a | MERGE matched | `MATCHED` |
| 3b | MERGE not-matched | `NOT MATCHED` |

**✅ Expected:** full=10.000, incremental=2.000 (request_id>8000)· watermark→10.000· merge upsert OK.

> 🧑‍🏫 **Trainer tip:** Η μεγάλη ερώτηση: *«γιατί να ξαναδιαβάσω 10.000 αν ήρθαν 2.000;»*. Τονίστε
> πότε full είναι ΟΚ (μικρά/dimension tables) vs incremental (fact tables, μεγάλος όγκος). Watermark = «πού έμεινα».

---

# 🟡 ΜΕΡΟΣ 2 — Auto Loader (~75')

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1a | Stream format | `cloudFiles` |
| 1b | File format | `csv` |
| 2a/3a | Checkpoint | `CKPT` (η μεταβλητή) |
| 2b/3b | Trigger | `availableNow` |

**✅ Expected:** batch1→6.000· batch2→10.000 (μόνο +4.000)· 4 αρχεία· checkpoint υπάρχει.

> 🧑‍🏫 **Trainer tip:** Το checkpoint είναι ο «αυτόματος watermark» — δείξτε ότι σβήνοντάς το θα
> ξαναδιάβαζε τα πάντα. Σύνδεση με Μέρος 1: ίδια ιδέα, αλλά διαχειρίζεται μόνο του τα αρχεία.

---

# 🟢 ΜΕΡΟΣ 3 — Streaming + foreachBatch MERGE (~60')

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1 | Streaming read format | `delta` |
| 2a | MERGE matched | `MATCHED` (UPDATE SET *) |
| 2b | MERGE not-matched | `NOT MATCHED` (INSERT *) |
| 3a | foreachBatch fn | `upsert_to_silver` |
| 3b | Trigger | `availableNow` |

**✅ Expected:** run1=10.000· μετά append run2=10.001· 0 διπλοεγγραφές.

> 🧑‍🏫 **Trainer tip:** Γιατί `foreachBatch`; Γιατί το MERGE είναι batch op — το `foreachBatch` σου
> δίνει ένα κανονικό DataFrame ανά micro-batch. Exactly-once = checkpoint + idempotent MERGE.

---

# 🏆 ΜΕΡΟΣ 4 — SCD Type 2 (Bonus, ~50')

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1 | mergeKey για insert rows | `None` (δεν ματσάρει ποτέ) |
| 2a | match μόνο τρέχουσες | `true` (is_current = true) |
| 2b | close παλιά version | `false` (is_current = false) |
| 2c | νέα version | `true` (is_current = true) |

**✅ Expected:** σύνολο 8· current 6· history 2· id=2 current=`rejected`, παλιά κλειστή.

> 🧑‍🏫 **Trainer tip:** Το «κόλπο»: 2 source rows ανά αλλαγή (mergeKey=id για close, mergeKey=null για insert).
> Ζωγραφίστε στο whiteboard τις 2 versions του id=2 με valid_from/valid_to.

---

## 🧹 Reset

```python
for t in ["kep_requests_src","kep_bronze_full","kep_bronze_incr","kep_watermark",
          "kep_bronze_autoloader","kep_stream_src","kep_silver_stream","dim_request_scd2"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
for p in ["kep_landing","_schemas/kep_autoloader","_checkpoints/kep_autoloader","_checkpoints/kep_silver_stream"]:
    dbutils.fs.rm(f"/Volumes/workspace/aade/aade_data/{p}", recurse=True)
```

## 🎯 Learning outcomes

full vs incremental· high-water-mark· append vs MERGE upsert· **Auto Loader** (cloudFiles/checkpoint/
schemaLocation/availableNow)· **Structured Streaming** (readStream/writeStream/foreachBatch MERGE/
exactly-once)· **SCD Type 2**. → Όλη η «Ημέρα 4: Incremental Ingestion & Streaming».
