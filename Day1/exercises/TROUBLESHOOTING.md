# 🛠️ Troubleshooting — Άσκηση Ημέρα 1

| Σφάλμα / Σύμπτωμα | Αιτία | Λύση |
|---|---|---|
| `Insufficient privileges to CREATE SCHEMA` | Δεν έχετε δικαίωμα στο catalog | Το `workspace` catalog συνήθως επιτρέπει schema creation. Αλλιώς → fallback `hive_metastore` (κάτω). |
| `CREATE CATALOG ... insufficient privileges` | Χρειάζεται metastore admin | Δεν δημιουργούμε catalog — χρησιμοποιούμε τον υπάρχοντα `workspace`. |
| `Volume does not exist` μετά το CREATE | Lag στο UC propagation | Περιμένετε ~30s, ξανατρέξτε το cell. |
| `Path does not exist: /Volumes/...` | Free Edition χωρίς Volume support | Fallback: `LANDING_PATH="/dbfs/FileStore/aade/landing"` + `dbutils.fs.mkdirs(LANDING_PATH)`. |
| `URLError` στο download cell | Δίκτυο / GitHub raw | Κατεβάστε χειροκίνητα τα CSV και upload στο Volume μέσω UI. |
| `Table not found: ...declarations_clean` (Μέρος 3) | Δεν τρέξατε το Μέρος 2 | Τρέξτε τα μέρη με τη σειρά (1→2→3). |
| `input_file_name() is not supported` | UC Standard cluster | Χρησιμοποιήστε `col("_metadata.file_path")` (έτσι είναι στη λύση). |
| `afm` βγαίνει `bigint` στο Silver | Ξεχάσατε το `cast("string")` | TODO 2a: `col("ΑΦΜ").cast("string").alias("afm")`. |
| `cannot resolve 'Κατηγορία_Φόρου'` | Ελληνικό όνομα/encoding | Τα CSV είναι UTF-8. Βεβαιωθείτε ότι διαβάζετε από το Bronze (που κράτησε τα ελληνικά ονόματα). |
| `GRANT` πετάει error | Free Edition χωρίς δικαιώματα | Το `try/except` το «πιάνει» — η σύνταξη είναι σωστή για production. |
| `PARSE_SYNTAX_ERROR` σε `USAGE` | Χρησιμοποιήσατε legacy `USAGE` | Σε UC είναι **`USE SCHEMA`**, όχι `USAGE`. |
| Self-check `❌ FAIL` σε exact νούμερο | Λάθος σε προηγούμενο TODO | Δείτε ποιο FAIL → ανατρέξτε στο αντίστοιχο TODO· σύγκριση με `_SOLUTION`. |
| `Schema already exists` | Re-run | Αγνοήστε — `IF NOT EXISTS` είναι idempotent. |

## Free Edition fallback (χωρίς Unity Catalog)

```python
CATALOG = "hive_metastore"
# Δημιουργήστε schemas με prefix αντί για 3-level catalog:
for s in ["aade_bronze","aade_silver","aade_gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
# Volumes δεν υπάρχουν σε hive_metastore → χρησιμοποιήστε DBFS:
LANDING_PATH = "/dbfs/FileStore/aade/landing"
import os; os.makedirs(LANDING_PATH, exist_ok=True)
# Παραλείψτε το CREATE VOLUME και τα SHOW VOLUMES.
```

## Reset (καθαρό ξεκίνημα)

```python
for t in ["aade_bronze.declarations_raw","aade_silver.declarations_clean","aade_gold.declarations_by_category_region"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.{t}")
print("✓ reset done")
```
