# 🛠️ Troubleshooting — Άσκηση Ημέρα 3 (Delta Lake)

| Σφάλμα / Σύμπτωμα | Αιτία | Λύση |
|---|---|---|
| `Table not found: ...tax_declarations_silver` | Δεν τρέξατε το Cell 0 του Μέρους 1 | Τρέξτε τα cells με τη σειρά· το Cell 0 χτίζει το base table. |
| `MERGE ... cannot resolve` | Λάθος στο `ON`/clauses | `ON t.declaration_id = s.declaration_id` · `WHEN MATCHED THEN UPDATE SET ...` · `WHEN NOT MATCHED THEN INSERT (...) VALUES (...)`. |
| `VERSION AS OF 0` → error / κενό | Το table ξαναχτίστηκε (νέο version 0) | Τρέξτε ξανά από το Cell 0 του Μέρους 2 (overwrite = νέο v0). |
| `RESTORE` δεν υποστηρίζεται | Πολύ παλιό runtime | DBR 10+ το έχει. Free Edition/Serverless ΟΚ. Εναλλακτικά `CREATE OR REPLACE TABLE ... AS SELECT * FROM t VERSION AS OF 0`. |
| `numFiles` δεν μειώθηκε μετά OPTIMIZE | Ήδη 1 file (μικρό dataset) | Τρέξτε πρώτα το small-files cell (6 micro-batches). Το self-check ελέγχει `after < before`. |
| `VACUUM` θέλει < 168h | Default retention guard | Αφήστε `DRY RUN` (ασφαλές). Για πραγματικό: `SET spark.databricks.delta.retentionDurationCheck.enabled=false` (⚠️ προσοχή production). |
| `table_changes`/`readChangeFeed` → empty ή error | CDF δεν ήταν enabled πριν τις αλλαγές | Το CDF καταγράφει **μόνο** αλλαγές μετά το enable. Τρέξτε: enable → capture `start_v` → αλλαγές → read από `start_v + 1`. |
| `_change_type` column δεν υπάρχει | Διαβάσατε χωρίς `readChangeFeed` | Προσθέστε `.option("readChangeFeed","true").option("startingVersion", start_v+1)`. |
| Incremental ≠ Full recompute | Λάθος πρόσημα στο TODO 3 | insert + update_postimage → `+amount`· delete + update_preimage → `−amount`. |
| Μικρή διαφορά (π.χ. 0.00x) incremental vs full | Floating-point order | Το self-check χρησιμοποιεί ανοχή `0.01€` — μην ανησυχείτε. |
| `Insufficient privileges` σε `ALTER TABLE` | Δεν είστε owner του table | Εσείς το φτιάξατε στο Cell 0 → είστε owner. Αν όχι, τρέξτε σε δικό σας schema. |
| Ελληνικά «σπασμένα» | Encoding | Τα CSV είναι UTF-8· το `display()` τα δείχνει σωστά. |

## Reset (καθαρό ξεκίνημα)

```python
for t in ["tax_declarations_silver","tax_declarations_tt","declarations_cdf","revenue_by_region_gold"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
print("✓ reset done")
```
