# ✅ Expected Outputs — Άσκηση Ημέρα 4 (Full vs Incremental)

Επαληθευμένα πάνω στο `kep_requests.csv` (10.000 αιτήματα, request_id 1..10000).

---

## Μέρος 1 — Full vs Incremental (~16 TODOs)

| Βήμα | Αποτέλεσμα |
|---|---|
| Source typed | `request_timestamp` = **timestamp**, + `request_date` |
| Audit log table | `etl_audit_log` δημιουργήθηκε· **≥ 3 runs** καταγράφονται |
| Full load | Bronze_full = **10.000** (processed 10.000)· validation gate περνά |
| Incremental append | processed = **2.000** (request_id > 8000)· watermark → 10.000 |
| Incremental MERGE | id=5 → `rejected`/wait=200· id=42 update· id 10001, 10002 insert |
| Reconciliation | incr distinct == full distinct == **10.000** |
| Cost report | FULL total_rows >> INCREMENTAL total_rows (~**80%** λιγότερη δουλειά) |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 1!`

---

## Μέρος 2 — Auto Loader (~15 TODOs)

| Βήμα | Αποτέλεσμα |
|---|---|
| Batch 1 (2 αρχεία) | Bronze = **6.000**· audit col `_source_file` υπάρχει |
| Batch 2 (re-run, ίδιο checkpoint) | Bronze = **10.000** (μόνο **+4.000**) |
| Batch 3 (extra στήλη `priority`) | Bronze = **10.100**· **schema drift → `_rescued_data`** ≥ 100 γραμμές |
| Silver | **5** service types (counts + avg_wait_min) |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 2!`

---

## Μέρος 3 — Streaming + foreachBatch (~14 TODOs)

| Βήμα | Αποτέλεσμα |
|---|---|
| Run 1 | Silver = **10.000**· Gold = **5** service types· batch log γράφει |
| Append (id=5 update + 2 versions του 10001) → Run 2 | Silver = **10.001** |
| **Dedup μέσα στο batch** | id=10001 κρατά τη **νέα** version → `rejected` |
| Run 3 (no new data) | Silver = **10.001** (idempotent — exactly-once) |
| Διπλοεγγραφές request_id | **0** |

Self-check → όλα `✅ OK`, `🎉 Τέλος Μέρους 3!`

> Τα streaming cells τρέχουν με `Trigger.AvailableNow` + `awaitTermination` → ολοκληρώνονται μόνα τους.

---

## Μέρος 4 — SCD Type 2 (~12 TODOs, Bonus)

Initial: 6 αιτήματα (version 1). Day 2: id2 (outcome), id3 (**μόνο wait** — δοκιμάζει το OR), id7 νέο.
Day 3: id2 ξανά (→ 3η version), id8 νέο.

| Μέτρηση | Τιμή |
|---|---|
| Total γραμμές | **11** |
| Current (`is_current=true`) | **8** (ids 1-8) |
| Historical (closed) | **3** (id2 v1, id2 v2, id3 v1) |
| id=2 versions | **3** (current version = 3) |
| id=3 (wait-only change) | **2** versions (αποδεικνύει multi-attribute detection) |
| 1 current ανά key | ✅ (no duplicates) |

Self-check → όλα `✅ OK`, `🏆 BONUS ΟΛΟΚΛΗΡΩΘΗΚΕ`.

---

## Tables που δημιουργούνται (`workspace.aade`)

`kep_requests_src`, `kep_bronze_full`, `kep_bronze_incr`, `kep_watermark`, `etl_audit_log`,
`kep_bronze_autoloader`, `kep_silver_by_service`, `kep_stream_src`, `kep_silver_stream`,
`kep_gold_service_live`, `kep_stream_batchlog`, `dim_request_scd2`.
