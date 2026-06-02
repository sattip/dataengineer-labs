# 🗓️ Day 1 — 3-Hour Lab Runsheet (Data Contracts στο επίκεντρο)

**Διάρκεια:** 180' (3 ώρες) · **Κοινό:** Μηχανικοί Δεδομένων (junior→mid) · **Mode:** Hands-on σε Databricks

Το αφήγημα της ημέρας είναι ένα **καθαρό arc**:

> **Θεμέλιο** (φτιάχνω catalog) → **Ποιότητα** (βάζω data contract μπροστά) → **Αξία** (ενώνω σε Citizen 360)

Ο **πυρήνας** είναι το νέο **Data Contracts lab** (80'). Δεξιά-αριστερά μπαίνουν δύο **υπάρχοντα** labs, κομμένα στον πυρήνα τους ώστε να χωρέσουν στις 3 ώρες.

---

## ⏱️ Χρονοδιάγραμμα (με την μία ματιά)

| Ώρα | Block | Διάρκεια | Notebook | Mode |
|---|---|---|---|---|
| 00:00 → 00:05 | 🎬 Welcome + setup check | 5' | — | Trainer |
| 00:05 → 00:40 | 🏗️ **Lab 1 — UC Foundation** (core only) | 35' | `Lab1_UC_Foundation.py` | Solo |
| 00:40 → 00:50 | ☕ Διάλειμμα | 10' | — | — |
| 00:50 → 02:10 | 🤝 **Data Contracts** (fill-in-the-blanks) ⭐ | 80' | `Lab_Data_Contracts.py` | Solo/ζευγάρια |
| 02:10 → 02:15 | 🧍 Stretch break | 5' | — | — |
| 02:15 → 02:55 | 🌐 **Lab 3 — Citizen 360** (core only) | 40' | `Lab3_Citizen_360_Discovery.py` | Ομάδες 2-3 |
| 02:55 → 03:00 | 🎓 Wrap-up + Q&A | 5' | — | Trainer |
| | **ΣΥΝΟΛΟ** | **180'** | | |

> ⚠️ Το **Lab 2 (Bronze Ingestion)** ΔΕΝ τρέχει σήμερα — το data-contract κομμάτι του (section 2.8 + Stretch 4) **αντικαθίσταται** από το αυτόνομο Data Contracts lab, που είναι πληρέστερο και self-contained.

---

## ✅ Πριν την τάξη (Trainer prep — 10' το πρωί)

1. **Import 3 notebooks** στο workspace (Workspace → Import → Upload File):
   - `Lab1_UC_Foundation.py`
   - `Lab_Data_Contracts.py`  ← το νέο
   - `Lab3_Citizen_360_Discovery.py`
   - (Κράτα κρυφά τα `*_SOLUTION.py` — answer keys.)
2. **Compute**: Serverless ή ένα μικρό cluster (DBR 14.3+). Όλα τα Step 0 είναι serverless-safe.
3. **Unity Catalog enabled** στο workspace. Αν όχι → δες fallback στο `Databricks_Setup_Guide.md`.
4. **Smoke test**: τρέξε εσύ το `Lab_Data_Contracts_SOLUTION.py` με **Run All** (~1') — πρέπει **3/3** verification ✅ (Silver=300, Quarantine=5, Audit≥8· τα governance tags είναι informational).
   - Αυτό προ-δημιουργεί `gt_lab` ώστε να μην περιμένουν όλοι το πρώτο `CREATE CATALOG`.
5. **Zoom chat**: έτοιμα τα 3 ονόματα notebooks + το μήνυμα «Import → Run Step 0 πρώτα».

> 💡 Self-contained: κανένα lab σήμερα δεν κατεβάζει δεδομένα από το internet. Το Data Contracts **παράγει** το dataset του και **γράφει** το YAML μόνο του. Δεν υπάρχει «δεν κατέβηκε το CSV» πρόβλημα.

---

## 🎬 00:00–00:05 · Welcome + setup check (5')

- Πες το arc της ημέρας (μία διαφάνεια): **Foundation → Contract → 360**.
- «Όλοι: Workspace → import τα 3 notebooks. Ανοίξτε το Lab 1 και πατήστε Run στο **Step 0** μόνο.»
- Όσο τρέχει το Step 0, εξήγησε το **3-level namespace** (`catalog.schema.table`).

---

## 🏗️ 00:05–00:40 · Lab 1 — UC Foundation, CORE ONLY (35')

**Notebook:** `Lab1_UC_Foundation.py` · **Σκοπός σήμερα:** να υπάρχει το `gt_lab` catalog για όλους.

| Κομμάτι | Κάνε; | Timebox |
|---|---|---|
| ΜΕΡΟΣ 1 — Θεωρία (1.1–1.6) | Διάβασέ το γρήγορα μαζί (μην το αφήσεις solo) | 8' |
| Step 0 — bootstrap | Ναι | (έτρεξε ήδη) |
| Steps 1–3 — catalog / 3 schemas / volume | **Ναι — ο πυρήνας** | 12' |
| Steps 4–6 — list files, SHOW, read CSV | Ναι (γρήγορα) | 10' |
| Verification | Ναι | 3' |
| **Stretch 1–3 + Super Stretch** | **SKIP σήμερα** (RBAC/explicit schema/widgets) | — |

**Coaching:** Όποιος τελειώσει νωρίς → ας ανοίξει το Stretch 2 (explicit schema) μόνος του, **χωρίς** να το παρουσιάσουμε.
**Pitfall:** «Catalog already exists» → δεν είναι error· έχουν `IF NOT EXISTS`. Προχώρα.

**Έξοδος του block:** όλοι έχουν `gt_lab.{bronze,silver,gold}` + `bronze.landing` volume. ✅

---

## ☕ 00:40–00:50 · Διάλειμμα (10')

---

## 🤝 00:50–02:10 · Data Contracts — fill-in-the-blanks ⭐ ΠΥΡΗΝΑΣ (80')

**Notebook:** `Lab_Data_Contracts.py` · **Answer key:** `Lab_Data_Contracts_SOLUTION.py`

### Πώς το παρουσιάζεις
Αυτό **δεν** είναι «γράψε από το μηδέν». Είναι **«συμπλήρωσε τα κενά»**: ο κορμός δίνεται, οι μαθητές
συμπληρώνουν 9 σημεία `____`, και **κάθε κενό έχει εξήγηση** (ΤΙ / ΓΙΑΤΙ / HINT) από πάνω.

> 🔑 Πες το ρητά: «Όπου βλέπετε `____`, αντικαταστήστε το. Αν το αφήσετε, θα δείτε
> `NameError: name '____' is not defined`. **Αυτό είναι το σινιάλο, όχι bug.**»

### Ροή (80')
| Φάση | Steps | Τι γίνεται | Timebox |
|---|---|---|---|
| **A — Θεωρία** | ΜΕΡΟΣ 1 (1.1–1.5) | Διάβασέ το **μαζί**. Κλειδιά: τα 3 outputs (Silver/Quarantine/Audit), schema vs quality rules, severity (error→quarantine, warning→flag). | 18' |
| **B — Bootstrap & δεδομένα** | Step 0, 1, 2, 2.5 | Run μαζί. Σταμάτα στο 2.5: «δες τα 5 κακά records — το καθένα σπάει διαφορετικό rule.» | 12' |
| **C — Τα κενά** | Steps 3–7 (TODO 1–9) | **Solo/ζευγάρια.** Trainer περπατά. Δες πίνακα κενών παρακάτω. | 35' |
| **D — Verification + debrief** | Verification + Σκέψεις | Run verification (3/3 ✅ + tags informational). Συζήτηση: γιατί quarantine, error vs warning. | 15' |

### Τα 9 κενά (κρυφό answer-key για τον trainer)
| TODO | Step | Concept | Λύση |
|---|---|---|---|
| 1 | 1 | Load YAML | `yaml.safe_load(f)` |
| 2 | 3 | **Schema drift** = set difference | `expected_cols - actual_cols` |
| 3 | 4 | fail = `NOT(expr)` | `raw_df.filter(f"NOT ({expr})").count()` |
| 4 | 5 | μόνο errors → quarantine | `"error"` |
| 5 | 5 | ένωση με OR | `" OR ".join([f"NOT ({e})" for e in error_rules])` |
| 6 | 5 | valid = αντίθετο | `raw_df.filter(f"NOT ({combined_fail})")` |
| 7a | 6 | full refresh | `"overwrite"` |
| 7b | 6 | target από contract | `contract["publishing"]["write_valid_to"]` |
| 8 | 6 | quarantine target | `contract["publishing"]["write_invalid_to"]` |
| 9a/9b | 7 | governance tags | `sec["classification"]` / `",".join(sec["pii_columns"])` |

### Σημεία που κολλάνε (πρόλαβέ τα)
- **TODO 2** μπερδεύουν τη φορά: `actual - expected` (λάθος) vs `expected - actual` (σωστό). Ρώτα: «τι _θέλω_ μείον τι _έχω_;»
- **TODO 3** δοκιμάζουν να μετρήσουν τα _σωστά_. Θύμισέ τους: το `expr` είναι αληθές για τα **σωστά** → fail = `NOT(...)`.
- **TODO 5** ξεχνούν το `NOT (...)` γύρω από κάθε expression.
- Ελληνικά ονόματα στηλών: αν γράψουν δικό τους filter, θέλει **backticks** (`` `ΑΦΜ` ``). Ο helper `quote_greek_columns` το κάνει ήδη — μην το αλλάξουν.

### Αναμενόμενο αποτέλεσμα
- Silver = **300**, Quarantine = **5**, Audit = **8 rules**.
- Στο Step 4 log: DQ001/2/3/5/6 δείχνουν 1 failure το καθένα· DQ004/7/8 → 0.

---

## 🧍 02:10–02:15 · Stretch break (5')

---

## 🌐 02:15–02:55 · Lab 3 — Citizen 360, CORE ONLY (40')

**Notebook:** `Lab3_Citizen_360_Discovery.py` · **Answer key:** `Lab3_..._SOLUTION.py`

> 🔗 **Γέφυρα:** «Φτιάξαμε contract-validated Silver. Τώρα ενώνουμε δεδομένα από 5 συστήματα σε **μία εικόνα ανά πολίτη** — εκεί κρύβεται η αξία.» Το Lab 3 έχει δικό του Step 0 (δικά του 5 CSVs με `afm` join key), οπότε τρέχει αυτόνομα.

| Κομμάτι | Κάνε; | Timebox |
|---|---|---|
| Θεωρία 3.1–3.7 (JOINs, aggregate-before-join, type consistency) | **Σύντομα** — κράτα 3.2 (AFM=join key), 3.4 (aggregate πριν join), 3.7 (cast) | 8' |
| Step 0–2 — bootstrap + inspect Bronze | Ναι | 6' |
| Steps 3–7 — aggregate ανά AFM + LEFT JOINs → citizen_360 | **Ναι — ο πυρήνας** | 18' |
| Step 8 — save στο Gold | Ναι | 3' |
| Step 9 — discovery analyses | 1-2 queries μόνο | 3' |
| Verification | Ναι | 2' |
| ΜΕΡΟΣ 4 Παρουσίαση (10'/ομάδα) + Stretch | **SKIP** (δεν χωράει σε 40') | — |

**Coaching:** Τόνισε το **aggregate-πριν-join** (Step 3.4) — η Νο1 αιτία row explosion.
**Pitfall:** KEP χρησιμοποιεί `citizen_afm` (όχι `afm`) — Step 5· πολλοί κάνουν join σε λάθος στήλη.

---

## 🎓 02:55–03:00 · Wrap-up (5')

Σύνδεσε το arc:
1. **Lab 1** → στήσαμε governance foundation (UC: catalog/schema/volume).
2. **Data Contracts** → βάλαμε «σύνορο ποιότητας»: τα καθαρά στο Silver, τα κακά σε καραντίνα, audit trail.
3. **Lab 3** → πάνω στο καθαρό data χτίσαμε Gold value (Citizen 360).

**Επόμενο:** Day 2 — Transformations & advanced quality· τα data contracts γίνονται **DLT expectations** και τα rules **monitored SLOs** (Day 5).

---

## 🧯 Plan B / time management

- **Πίσω στον χρόνο μετά το Data Contracts;** Κόψε το Lab 3 στα Steps 3–8 και κάνε Step 9 demo από το SOLUTION.
- **Μπροστά στον χρόνο;** Άνοιξε **Stretch 1** του Data Contracts (νέο rule `Βάση_Φόρου > 0`) — 10' γρήγορη νίκη.
- **Κάποιος κόλλησε >10' σε ένα `____`;** Δείξε *μόνο* τη γραμμή-λύση από τον πίνακα παραπάνω, όχι όλο το SOLUTION.
- **Mixed ταχύτητες;** Οι γρήγοροι βοηθούν τους αργούς (pair) — μετράει σαν teach-back.

---

## 📦 Αρχεία αυτού του block

| Αρχείο | Ρόλος |
|---|---|
| `Lab1_UC_Foundation.py` (+ `_SOLUTION`) | Filler A — foundation |
| `Lab_Data_Contracts.py` (+ `_SOLUTION`) | ⭐ Πυρήνας — fill-in-the-blanks |
| `Lab3_Citizen_360_Discovery.py` (+ `_SOLUTION`) | Filler B — discovery |
| `RUNSHEET_Day1_3h_DataContracts.md` | Αυτό το έγγραφο |
