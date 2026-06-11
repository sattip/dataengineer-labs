# ✅ Expected Outputs — Άσκηση Ημέρα 4 (Full vs Incremental)

Επαληθευμένα πάνω στο `kep_requests.csv` (10.000 αιτήματα, request_id 1..10000).

---

## Μέρος 1 — Full vs Incremental

| Βήμα | Αποτέλεσμα |
|---|---|
| Source | **10.000** |
| TODO 1 Full load | Bronze_full = **10.000** (επεξεργάστηκε 10.000) |
| Initial incremental state | Bronze_incr = 8.000, watermark = 8.000 |
| TODO 2 Incremental append | επεξεργάζεται **2.000** (request_id > 8000) → Bronze_incr = 10.000, watermark = 10.000 |
| TODO 3 MERGE upsert | id=5 → `rejected` (update)· id 10001, 10002 → insert |

**Το μάθημα:** full load = 10.000 rows· incremental = 2.000 rows → **~80% λιγότερη δουλειά**.
Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 1!`

---

## Μέρος 2 — Auto Loader

| Βήμα | Αποτέλεσμα |
|---|---|
| Batch 1 (2 αρχεία, 6.000) → run | Bronze = **6.000** |
| Batch 2 (2 νέα αρχεία) → re-run (ίδιο checkpoint) | Bronze = **10.000** (μόνο **+4.000** νέα) |
| Landing files | 4 CSV· checkpoint δημιουργήθηκε |

**Το μάθημα:** το checkpoint = αυτόματο watermark· τα ήδη-διαβασμένα αρχεία **δεν** ξαναδιαβάζονται.
Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 2!`

---

## Μέρος 3 — Streaming + foreachBatch MERGE

| Βήμα | Αποτέλεσμα |
|---|---|
| Run 1 (initial stream) | Silver = **10.000** (distinct request_id) |
| Append (id=5 update + id=10001 new) → run 2 | Silver = **10.001** (id=5 updated, id=10001 inserted) |
| Διπλοεγγραφές request_id | **0** (το MERGE κρατά μοναδικότητα) |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 3!`

> Σημ.: τα streaming cells τρέχουν με `Trigger.AvailableNow` + `awaitTermination` → ολοκληρώνονται
> μόνα τους (δεν μένουν «running» για πάντα).

---

## Μέρος 4 — SCD Type 2 (Bonus)

Initial: 5 αιτήματα (όλα current). Changes: id 2,3 άλλαξαν· id 6 νέο.

| Μέτρηση | Τιμή |
|---|---|
| Σύνολο γραμμών | **8** (5 + 2 νέες versions + 1 νέο) |
| Τρέχουσες (`is_current=true`) | **6** (ids 1-6) |
| Ιστορικές (`is_current=false`) | **2** (παλιές versions των 2,3) |
| id=2 τρέχον outcome | `rejected` |
| id=2 παλιά version | κλειστή (`valid_to` set) |

Self-check → όλα `✅ OK`, `🏆 BONUS ΟΛΟΚΛΗΡΩΘΗΚΕ`.

Τελικά tables στο `workspace.aade`: `kep_requests_src`, `kep_bronze_full`, `kep_bronze_incr`,
`kep_watermark`, `kep_bronze_autoloader`, `kep_stream_src`, `kep_silver_stream`, `dim_request_scd2`.
