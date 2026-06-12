# 🔁 Άσκηση Ημέρα 4 — Full Load vs Incremental (Fill-in-the-Blank)

> **Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ**
> Σειρά **4 μερών** στο πιο κρίσιμο ingestion δίλημμα: **full load vs incremental load**, με
> Auto Loader, Structured Streaming, και SCD2. **~4.5 ώρες · ~57 TODOs** πραγματικού κώδικα.
>
> 💡 Μαζί με την **Ημέρα 3 (Delta: MERGE/CDF/time travel)** καλύπτουν ένα **6ωρο+ Day 3+4**
> με κοινό νήμα «incremental data processing».

## 🎓 Φιλοσοφία

Σε κάθε βήμα: 🧠 ΕΝΝΟΙΑ (τι/γιατί/πώς) → ✍️ TODO (συμπληρώνετε τα `_____`) → self-check.
Κάθε μέρος είναι **ουσιαστικό** notebook (όχι λίγες γραμμές): audit logs, validation gates,
reconciliation, schema drift, dedup, metrics — όπως σε πραγματικό pipeline.

## 📂 Περιεχόμενα

| Αρχείο | Θέμα | Διάρκεια | TODOs |
|---|---|---|---|
| `Exercise1_FullVsIncremental_STARTER.py` | **Full** vs **incremental** + audit log + reconciliation | ~80' | ~16 |
| `Exercise2_AutoLoader_STARTER.py` | **Auto Loader** + audit cols + **schema drift/rescued** + Silver agg | ~80' | ~15 |
| `Exercise3_Streaming_Merge_STARTER.py` | **Streaming** `foreachBatch` (dedup + MERGE + Gold + exactly-once) | ~70' | ~14 |
| `Exercise4_SCD2_Bonus_STARTER.py` | **SCD Type 2** — versioned history, multi-attribute, 2 batches | ~60' | ~12 |

Κάθε `_STARTER` έχει `_SOLUTION` (πλήρης, σχολιασμένη). Συνοδευτικά: `STEP_BY_STEP_Exercises.md`
(hints ανά TODO + trainer tips), `EXPECTED_OUTPUTS.md`, `TROUBLESHOOTING.md`.

## 🗺️ Το νήμα: full → incremental → streaming → SCD2

```
Μέρος 1  Full (overwrite, ALL)  vs  Incremental (watermark, NEW)  + audit log + reconciliation
Μέρος 2  Auto Loader: checkpoint = αυτόματο watermark · schema drift → rescued data · Silver agg
Μέρος 3  Streaming foreachBatch: dedup → MERGE → Gold → metrics · exactly-once
Μέρος 4  SCD2: incremental ΜΕ ιστορικό (version/valid_from/valid_to/is_current)
```

## 🎯 Μαθησιακοί στόχοι

**Μέρος 1** — typed ingestion· **audit log/metrics**· high-water-mark· append vs MERGE upsert (multi-column)· validation gate· reconciliation & cost report.
**Μέρος 2** — `cloudFiles` (schemaLocation/inferColumnTypes/**rescuedDataColumn**/schemaEvolutionMode)· audit cols (`_metadata.file_path`)· checkpoint· `Trigger.AvailableNow`· **schema drift**· Silver aggregation.
**Μέρος 3** — `readStream`· **in-batch dedup** (Window)· **`foreachBatch`** (MERGE upsert + running Gold + batch metrics)· **exactly-once** (3 runs).
**Μέρος 4** — SCD Type 2 (2-part `mergeKey` trick)· multi-attribute change detection· version numbering· history query.

## ⚙️ Προαπαιτούμενα

- Databricks workspace (Free Edition + Serverless — υποστηρίζει Auto Loader & streaming).
- Catalog/Schema `workspace.aade` + Volume `aade_data` (auto). Dataset: `kep_requests.csv` (10.000 αιτήματα ΚΕΠ).
- Το Cell 0 κάθε notebook κατεβάζει το CSV από GitHub.
- Τρέξτε **με τη σειρά**. Τα Μέρη 2 & 3 χρησιμοποιούν streaming → αφήστε τα cells να **ολοκληρωθούν** (`awaitTermination`).

## 🔗 Σχέση με Ημέρα 3

Η Ημέρα 3 έδειξε το incremental **στο Delta** (MERGE/CDF). Η Ημέρα 4 το πάει στο **ingestion**
(full vs incremental, Auto Loader, streaming, SCD2). Μαζί = ολοκληρωμένη εικόνα incremental pipelines.

---

➡️ Ξεκινήστε από `Exercise1_FullVsIncremental_STARTER.py`. Hints → `STEP_BY_STEP_Exercises.md`.
