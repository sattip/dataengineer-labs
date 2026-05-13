# 📚 Day 6 — Capstone 3-Hour Workshop

**Audience:** Data Engineers που έχουν παρακολουθήσει Days 1-5
**Difficulty:** ⭐⭐⭐⭐ Expert (revision + synthesis)
**Format:** Self-paced exercises με TODO blocks + trainer reviews

## 🗓️ Timeline (3 hours)

| Time | Block | Activity | File |
|---|---|---|---|
| 0:00–0:25 | Reading | Self-study revision (25 min) | `00_Reading_Material.md` |
| 0:25–0:30 | Q&A | Quick clarifications | — |
| 0:30–1:30 | Block A | 4 foundation exercises | `01_Exercises_Foundations.py` |
| 1:30–1:35 | Break | 5 min | — |
| 1:35–2:35 | Block B | 4 mid-level exercises | `02_Exercises_MidLevel.py` |
| 2:35–2:40 | Break | 5 min | — |
| 2:40–3:00 | Capstone | Mini end-to-end project | `03_Capstone_MiniProject.py` |
| 3:00–3:15 | Presentation | Trainees παρουσιάζουν (5 min each) | — |

> **Σημείωση**: αν το Capstone παίρνει 30 min ως planned, η συνολική διάρκεια είναι 3:15h.
> Για κανονικά 3h, μπορείτε να συντομεύσετε το reading σε 15 min ή να αφήσετε τις
> παρουσιάσεις προαιρετικές.

## 📦 Files

| File | Type | Description |
|---|---|---|
| `00_Reading_Material.md` | Reference | Συνοπτικός οδηγός όλων των concepts (9 sections) |
| `01_Exercises_Foundations.py` | Notebook | 4 exercises με TODO blocks + verifications |
| `02_Exercises_MidLevel.py` | Notebook | 4 mid-level exercises (streaming + Delta features) |
| `03_Capstone_MiniProject.py` | Notebook | End-to-end ΑΑΔΕ pipeline (no hints) |
| `04_Solutions.py` | Notebook | Όλες οι λύσεις (κρυφό για τους trainees!) |
| `README.md` | Guide | Αυτό το αρχείο |

## 🎯 Exercise Summary

### Block A: Foundations (60 min)
| # | Exercise | Concept | Time |
|---|---|---|---|
| 1 | Ingest CSV → Bronze Delta | Read files + metadata | 12' |
| 2 | Data Quality + Quarantine | Filter + log invalid | 12' |
| 3 | Silver upsert με MERGE | DeltaTable conditional merge | 18' |
| 4 | Gold aggregation με Window | Window functions + groupBy | 18' |

### Block B: Mid-Level (60 min)
| # | Exercise | Concept | Time |
|---|---|---|---|
| 5 | Auto Loader streaming Bronze | Incremental file ingestion | 15' |
| 6 | foreachBatch + MERGE | Streaming + transactional upsert | 20' |
| 7 | Schema Evolution | New columns auto-merge | 12' |
| 8 | Time Travel + RESTORE | Delta versioning, rollback | 13' |

### Capstone: Audit Priority (30 min)
End-to-end pipeline: 2 sources (TAXIS + myDATA) → Silver με DQ → Gold top-100 suspects.

## 🎓 Learning Outcomes

Μετά το workshop, οι trainees θα μπορούν:

- [ ] Να γράψουν Bronze ingestion με metadata + TBLPROPERTIES
- [ ] Να εφαρμόσουν quarantine pattern για data quality
- [ ] Να γράψουν conditional MERGE (whenMatchedUpdate με condition)
- [ ] Να συνδυάσουν Window functions με groupBy aggregations
- [ ] Να στήσουν Auto Loader streaming pipeline
- [ ] Να χρησιμοποιήσουν foreachBatch + MERGE σε streaming context
- [ ] Να ενεργοποιήσουν schema evolution
- [ ] Να κάνουν time travel + RESTORE σε disaster scenarios
- [ ] Να σχεδιάσουν end-to-end pipeline από scratch

## 🧰 Prerequisites

### Για τους Trainees
- Databricks Free Edition account
- Workspace catalog accessible
- Έχουν παρακολουθήσει Days 1-5
- Διαβάσει το `00_Reading_Material.md` ΠΡΙΝ το workshop (ή πρώτα 25 min in-class)

### Για τον Trainer
- Run τα 04_Solutions.py σε δικό σας workspace ως smoke test
- Επιβεβαίωση ότι Auto Loader works σε Free Edition (Serverless)
- Backup πλάνο: εάν Free Edition έχει issues με streaming, switch σε classic batch reads

## 📋 Trainer Talking Points

### Opening (5 min πριν Block A)
> «Καλωσήρθατε στην τελευταία ημέρα. Αντί για άλλα concepts, σήμερα **εφαρμόζουμε**
> όλα όσα μάθαμε. Έχετε ένα reading material — διαβάστε το για 25 min — και μετά
> 8 hands-on exercises σε 2 hours. Θα γράψετε εσείς τον κώδικα. Τέλος, μίνι
> capstone project: ένα ΑΑΔΕ pipeline από scratch.»

### Pacing tips
- Block A: αν μαθητές κολλάνε στο MERGE (Exercise 3), δείξτε live το **condition pattern** στο board
- Block B: Exercise 7 (schema evolution) ίσως απαιτεί 2 retries — προειδοποιήστε
- Capstone: αν τελειώσει νωρίς, ζητείστε να συζητήσουν "what would break in production"

### Common stumbling blocks

| Πρόβλημα | Λύση |
|---|---|
| Ξεχάστηκε `YOUR_NAME` change | Re-run πρώτου cell |
| Schema conflicts | `DROP SCHEMA ... CASCADE` και ξεκίνα από την αρχή |
| Streaming queries hang | `availableNow=True` πάντα, `awaitTermination()` |
| MERGE σε empty table | Πρώτα `CREATE TABLE` shell, μετά MERGE |
| Schema evolution exception | Retry — αυτό είναι expected behavior |
| `input_file_name()` error | Use `col("_metadata.file_path")` |

## 🧯 Post-Workshop

### Suggested follow-up
1. Convert Capstone σε **DLT pipeline** (declarative version)
2. Add **Unity Catalog GRANTs** σε analyst group
3. Build **observability dashboard** πάνω από `audit_priority_top100`
4. Setup **scheduled job** για daily run

### Assessment rubric
| Criterion | Pass | Fail |
|---|---|---|
| Block A completion | ≥3/4 exercises pass verifications | ≤2 |
| Block B completion | ≥3/4 exercises pass verifications | ≤2 |
| Capstone | Pipeline runs end-to-end, output makes sense | Missing layers ή errors |
| Code quality | Clear naming, docstrings, no obvious anti-patterns | Spaghetti |
| Presentation | Coherent flow, defends design choices | Cannot explain |

**Pass rate**: 4/5 criteria → Certified

## 🔗 GitHub Raw URLs

```
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Capstone_3h_Workshop/00_Reading_Material.md
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Capstone_3h_Workshop/01_Exercises_Foundations.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Capstone_3h_Workshop/02_Exercises_MidLevel.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Capstone_3h_Workshop/03_Capstone_MiniProject.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day6/Capstone_3h_Workshop/04_Solutions.py
```

---

> *«Ο καλύτερος τρόπος να μάθεις data engineering είναι να γράφεις pipelines.
> Όχι να βλέπεις διαφάνειες, να γράφεις pipelines.»*
