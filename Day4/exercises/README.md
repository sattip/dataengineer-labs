# 🔁 Άσκηση Ημέρα 4 — Full Load vs Incremental (Fill-in-the-Blank)

> **Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ**
> Σειρά **4 μερών** στο πιο κρίσιμο ingestion δίλημμα: **full load vs incremental load**, με
> Auto Loader, Structured Streaming, και SCD2. Διάρκεια: **~4 ώρες**.
>
> 💡 Μαζί με την **Ημέρα 3 (Delta: MERGE/CDF/time travel)** καλύπτουν ένα **6ωρο** Day 3+4
> με κοινό νήμα «incremental data processing».

## 🎓 Φιλοσοφία

Σε κάθε βήμα: 🧠 ΕΝΝΟΙΑ (τι/γιατί/πώς) → ✍️ TODO (συμπληρώνετε τα `_____`) → self-check.

## 📂 Περιεχόμενα

| Αρχείο | Θέμα | Διάρκεια |
|---|---|---|
| `Exercise1_FullVsIncremental_STARTER.py` | **Full load** vs **incremental** (watermark append + MERGE upsert) | ~75' |
| `Exercise2_AutoLoader_STARTER.py` | **Auto Loader** — incremental ingestion αρχείων (checkpoint) | ~75' |
| `Exercise3_Streaming_Merge_STARTER.py` | **Structured Streaming** + `foreachBatch` MERGE (streaming upsert) | ~60' |
| `Exercise4_SCD2_Bonus_STARTER.py` | **SCD Type 2** — incremental με ιστορικό | ~50' |

Κάθε `_STARTER` έχει `_SOLUTION`. Συνοδευτικά: `STEP_BY_STEP_Exercises.md`, `EXPECTED_OUTPUTS.md`, `TROUBLESHOOTING.md`.

## 🗺️ Το νήμα: full → incremental → streaming → SCD2

```
Μέρος 1  Full load (overwrite, processes ALL)  vs  Incremental (watermark, processes NEW)
Μέρος 2  Auto Loader: το checkpoint = αυτόματο watermark για ΑΡΧΕΙΑ που προσγειώνονται
Μέρος 3  Streaming: συνεχές incremental + foreachBatch MERGE = exactly-once upsert
Μέρος 4  SCD2: incremental ΜΕ ιστορικό (valid_from/valid_to/is_current)
```

## 🎯 Μαθησιακοί στόχοι

**Μέρος 1** — full load (overwrite) vs incremental· high-water-mark table· append vs MERGE upsert· το trade-off κόστους (rows processed).
**Μέρος 2** — `cloudFiles`· `checkpointLocation` (η μνήμη)· `schemaLocation`· `Trigger.AvailableNow`· exactly-once file processing.
**Μέρος 3** — `readStream`/`writeStream`· **`foreachBatch` + MERGE**· streaming upsert· idempotency/exactly-once.
**Μέρος 4** — SCD Type 2 MERGE pattern (2-part source με `mergeKey`)· close-old + insert-new· ιστορικό versions.

## ⚙️ Προαπαιτούμενα

- Databricks workspace (Free Edition + Serverless — υποστηρίζει Auto Loader & streaming).
- Catalog/Schema `workspace.aade` + Volume `aade_data` (auto). Dataset: `kep_requests.csv` (10.000 αιτήματα ΚΕΠ).
- Το Cell 0 κάθε notebook κατεβάζει το CSV από GitHub.
- Τρέξτε **με τη σειρά**. Τα Μέρη 2 & 3 χρησιμοποιούν streaming → αφήστε τα cells να **ολοκληρωθούν** (`awaitTermination`).

## 🔗 Σχέση με Ημέρα 3

Η Ημέρα 3 έδειξε το incremental **στο Delta** (MERGE/CDF). Η Ημέρα 4 το πάει στο **ingestion**
(full vs incremental, Auto Loader, streaming). Μαζί = ολοκληρωμένη εικόνα incremental pipelines.

---

➡️ Ξεκινήστε από `Exercise1_FullVsIncremental_STARTER.py`. Hints → `STEP_BY_STEP_Exercises.md`.
