# ✅ Expected Outputs — Άσκηση Ημέρα 3 (Delta Lake)

Επαληθευμένα πάνω στο `declarations.csv` (300 δηλώσεις TAXIS).

---

## Μέρος 1 — Delta DML + MERGE

| Βήμα | Αποτέλεσμα |
|---|---|
| Base Silver | **300** γραμμές, `format = delta` |
| TODO 2 UPDATE | `declaration_id=1` → `status = 'Εγκεκριμένη'` |
| TODO 3 DELETE | `declaration_id=2` φεύγει → **299** γραμμές |
| TODO 4 MERGE | 3 matched (3,4,5 → update) + 2 inserts (9001,9002) → **301** γραμμές |
| TODO 5 ALTER | προστίθεται στήλη `review_note` |
| History | ≥ 4 versions (WRITE, UPDATE, DELETE, MERGE, …) |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 1!`

---

## Μέρος 2 — Time Travel + Maintenance

| Βήμα | Αποτέλεσμα |
|---|---|
| Build fresh (`tax_declarations_tt`) v0 | **300** |
| UPDATE + DELETE (`status='Απορριφθείσα'`, 34 rows) | current = **266** |
| TODO 2 Time travel `VERSION AS OF 0` | **300** (ενώ current 266) |
| TODO 3 `RESTORE TO VERSION AS OF 0` | πίσω στις **300** |
| Small files (6 micro-batches) | `numFiles` αυξάνεται (π.χ. 7+), rows = 306 |
| TODO 4 `OPTIMIZE` | `numFiles` πέφτει δραστικά (συνήθως **1**) — `files_after < files_before` |
| TODO 4 `ZORDER BY (region)` | καταγράφεται OPTIMIZE operation στο history |
| TODO 5 `VACUUM ... DRY RUN` | λίστα υποψήφιων files (τίποτα δεν σβήνεται με 168h) |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 2!`

> Σημ.: ο ακριβής αριθμός `numFiles` πριν το OPTIMIZE ποικίλλει — το self-check ελέγχει
> `files_after < files_before`, όχι απόλυτη τιμή.

---

## Μέρος 3 — Change Data Feed + Incremental

| Βήμα | Αποτέλεσμα |
|---|---|
| TODO 1 enable CDF | `delta.enableChangeDataFeed = true` |
| Αλλαγές | 2 inserts (9001,9002), 2 updates (10,11 +1000), 2 deletes (20,21) |
| TODO 2 change feed | rows με `_change_type` ∈ {`insert`, `update_preimage`, `update_postimage`, `delete`} |
| TODO 3 net_delta | signed sum ανά region (insert/postimage `+`, delete/preimage `−`) |
| TODO 4 MERGE | Gold ενημερώνεται incrementally |
| **Validation** | **Incremental Gold == Full recompute** (ανά region, ανοχή 0.01€) |

Self-check:
```
✅ OK — CDF ενεργοποιήθηκε
✅ OK — Change feed έχει _change_type
✅ OK — Υπάρχουν insert+update+delete
✅ OK — Incremental Gold == Full recompute
🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 3!
```

Τελικά tables στο `workspace.aade`: `tax_declarations_silver`, `tax_declarations_tt`,
`declarations_cdf`, `revenue_by_region_gold`.
