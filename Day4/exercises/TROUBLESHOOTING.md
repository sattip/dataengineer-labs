# 🛠️ Troubleshooting — Άσκηση Ημέρα 4 (Full vs Incremental / Streaming)

| Σφάλμα / Σύμπτωμα | Αιτία | Λύση |
|---|---|---|
| Streaming cell «κολλάει» / τρέχει για πάντα | Ξεχάσατε `Trigger.AvailableNow` ή `awaitTermination` | Με `trigger(availableNow=True)` + `q.awaitTermination()` το batch τελειώνει μόνο του. |
| `cloudFiles` not found / format error | Λάθος στο `.format` | `spark.readStream.format("cloudFiles").option("cloudFiles.format","csv")`. |
| `schemaLocation` required | Auto Loader θέλει schema location | Δώστε `.option("cloudFiles.schemaLocation", SCHEMA_LOC)` (path σε Volume). |
| `checkpointLocation` must be set | writeStream χωρίς checkpoint | `.option("checkpointLocation", CKPT)` — απαραίτητο για incremental/exactly-once. |
| Batch 2 ξαναδιάβασε ΟΛΑ τα αρχεία | Διαφορετικό/σβησμένο checkpoint | Χρησιμοποιήστε το **ίδιο** `CKPT` στα δύο runs. Μην το σβήνετε ανάμεσα. |
| `Table not found` (Μέρος 1/3) | Δεν τρέξατε το Cell 0 | Τρέξτε τα cells με τη σειρά· το Cell 0 χτίζει source/silver. |
| MERGE `multiple source rows matched` | Διπλά keys στο source batch | Κάντε dedup στο source πριν το MERGE (`dropDuplicates(["request_id"])`). |
| Streaming Delta source: `not append-only` | Έγινε overwrite/update στο source μετά την έναρξη | Στο Μέρος 3 το source δέχεται **μόνο append**. Για updates → `.option("ignoreChanges","true")`. |
| Append schema mismatch (Μέρος 3) | Νέες γραμμές με λάθος schema | Φτιάξτε τις από υπάρχουσες (`spark.table(SRC).filter(...).withColumn(...)`) ώστε να ταιριάζει. |
| `mergeSchema` / missing columns σε append | DataFrame με υποσύνολο στηλών | Είτε δώστε όλες τις στήλες, είτε `.option("mergeSchema","true")` (για ΝΕΕΣ στήλες, όχι missing). |
| Auto Loader σε Free Edition δεν τρέχει | Σπάνιο — περιορισμός runtime | Fallback: `spark.read.format("csv")` σε loop με watermark (Μέρος 1 pattern). |
| `_rescued_data` column εμφανίζεται | Auto Loader βάζει εκεί ό,τι δεν ταίριαξε στο schema | Φυσιολογικό· περιέχει «διασωθέντα» πεδία. Αγνοήστε ή κάντε drop. |
| SCD2: λάθος αριθμός versions | Λάθος `mergeKey`/flags | insert rows: `mergeKey = None`· match: `is_current = true`· close: `is_current=false`· new: `is_current=true`. |
| Ελληνικά «σπασμένα» | Encoding | UTF-8· το `display()` τα δείχνει σωστά. |

## 🧹 Reset (καθαρό ξεκίνημα)

```python
for t in ["kep_requests_src","kep_bronze_full","kep_bronze_incr","kep_watermark",
          "kep_bronze_autoloader","kep_stream_src","kep_silver_stream","dim_request_scd2"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
for p in ["kep_landing","_schemas/kep_autoloader","_checkpoints/kep_autoloader","_checkpoints/kep_silver_stream"]:
    dbutils.fs.rm(f"/Volumes/workspace/aade/aade_data/{p}", recurse=True)
print("✓ reset done")
```
