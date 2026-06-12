# 📥 Μέρος 2 — Auto Loader: Αναλυτική Εξήγηση

> Συνοδευτικός οδηγός για το `Exercise2_AutoLoader_STARTER.py` (Ημέρα 4 · ~80').
> Για τον **εκπαιδευτή** (να καταλάβει βαθιά & να το διδάξει) και τον **εκπαιδευόμενο**
> (να καταλάβει το *γιατί* πίσω από κάθε γραμμή).

---

## 1. Το πρόβλημα του πραγματικού κόσμου

Τα **ΚΕΠ** εξάγουν τα αιτήματα πολιτών σε **αρχεία CSV** που «προσγειώνονται» σε έναν φάκελο
(landing zone) **συνεχώς**: σήμερα 2 αρχεία, αύριο άλλα 2, μεθαύριο άλλα 3…

Το ερώτημα-κλειδί κάθε φορά που τρέχει το pipeline:

> **«Ποια αρχεία είναι ΚΑΙΝΟΥΡΙΑ;»** Πρέπει να διαβάσω **μόνο αυτά** — αλλιώς θα ξαναδιαβάσω
> τα παλιά και θα δημιουργήσω **διπλοεγγραφές** (duplicates) στις αναφορές προς τη Διοίκηση.

Στο **Μέρος 1** το έλυνες **χειροκίνητα** με *watermark* (κρατούσες εσύ σε ένα table το «μέχρι
ποιο id έφτασα»). Δουλεύει, αλλά πρέπει να το γράψεις και να το συντηρήσεις **εσύ**.

Ο **Auto Loader** το κάνει **αυτόματα**. Εσύ λες απλώς «διάβασε αυτόν τον φάκελο» — αυτός
θυμάται μόνος του τι έχει ήδη διαβάσει.

---

## 2. Τι είναι ο Auto Loader (σε μία πρόταση)

> Ο **Auto Loader** (`format("cloudFiles")`) είναι μια μηχανή **incremental ingestion αρχείων**:
> ανιχνεύει τα νέα αρχεία που φτάνουν σε έναν φάκελο και επεξεργάζεται **μόνο αυτά**, **ακριβώς
> μία φορά** (exactly-once), κρατώντας τη «μνήμη» του σε ένα **checkpoint**.

---

## 3. Walkthrough — βήμα προς βήμα (με τον κώδικα)

### Cell 0 — Setup & «προσγείωση» 1ου batch (έτοιμο)
Δημιουργεί schema/volume, κατεβάζει το `kep_requests.csv`, **καθαρίζει** τυχόν παλιά κατάσταση
(landing, schemaLocation, checkpoint, Bronze table) για επαναληψιμότητα, και γράφει το **1ο batch**:
2 αρχεία (6.000 γραμμές) στον φάκελο `kep_landing`.

```python
LANDING    = f"{VOLUME}/kep_landing"                       # ο φάκελος που "ακούει"
SCHEMA_LOC = f"{VOLUME}/_schemas/kep_autoloader"           # πού κρατά το schema
CKPT       = f"{VOLUME}/_checkpoints/kep_autoloader"       # η "μνήμη" (checkpoint)
```

> 💡 **Γιατί καθαρίζουμε στην αρχή;** Για να μπορείς να τρέξεις το notebook ξανά από την αρχή
> με καθαρά αποτελέσματα. Σε production **δεν** το κάνεις αυτό — το checkpoint πρέπει να επιβιώνει.

---

### TODO 1 — Ο reader (`cloudFiles`) + audit columns

```python
spark.readStream
    .format("cloudFiles")                                  # ← κάνει τη ροή Auto Loader
    .option("cloudFiles.format", "csv")                    # τύπος αρχείων στο landing
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)       # θυμάται/εξελίσσει το schema
    .option("cloudFiles.inferColumnTypes", "true")         # μάντεψε τύπους (όχι όλα string)
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # "συρτάρι διάσωσης" (δες §6)
    .option("cloudFiles.schemaEvolutionMode", "rescue")    # νέα στήλη → rescued, ΟΧΙ crash
    .option("header", "true")
    .load(LANDING)
    .withColumn("_source_file", col("_metadata.file_path"))  # audit: από ΠΟΙΟ αρχείο
    .withColumn("_ingested_at", current_timestamp())         # audit: ΠΟΤΕ ήρθε
```

**Κάθε option, τι κάνει:**
| Option | Ρόλος |
|---|---|
| `format("cloudFiles")` | ενεργοποιεί τον Auto Loader (αντί απλού `read`) |
| `cloudFiles.format` | τι τύπος αρχείων διαβάζει (csv/json/parquet…) |
| `cloudFiles.schemaLocation` | φάκελος όπου **θυμάται το schema** (για να ξέρει αν άλλαξε) |
| `cloudFiles.inferColumnTypes` | μαντεύει σωστούς τύπους (π.χ. int αντί string) |
| `cloudFiles.rescuedDataColumn` | στήλη όπου «διασώζονται» πεδία που δεν ταιριάζουν |
| `cloudFiles.schemaEvolutionMode` | τι κάνει σε νέα στήλη (`rescue` = μη-καταστροφικό) |

> ⚠️ **Audit metadata:** το `_metadata.file_path` είναι το σωστό (UC-safe) — **όχι** το παλιό
> `input_file_name()` που δεν δουλεύει σε Unity Catalog clusters.

---

### TODO 2 — Γράψιμο στο Bronze με checkpoint

```python
.writeStream.format("delta")
    .option("checkpointLocation", CKPT)     # ← η ΜΝΗΜΗ: ποια αρχεία διάβασα
    .option("mergeSchema", "true")          # επίτρεψε αλλαγές schema στο Bronze
    .trigger(availableNow=True)             # επεξεργάσου ό,τι υπάρχει & σταμάτα
    .toTable(BRONZE)
q.awaitTermination()                        # περίμενε να ολοκληρωθεί το batch
```

**Τα 2 πιο σημαντικά εδώ:**
- **`checkpointLocation`** → χωρίς αυτό **δεν υπάρχει incremental**. Είναι ο σελιδοδείκτης.
- **`trigger(availableNow=True)`** → «τρέξε μία φορά πάνω σε ό,τι υπάρχει τώρα και σταμάτα».
  Ιδανικό για notebook/batch job (δεν μένει να τρέχει για πάντα). Το `awaitTermination()`
  περιμένει να τελειώσει πριν προχωρήσει το επόμενο cell.

**Αποτέλεσμα Run 1:** Bronze = **6.000** γραμμές.

---

### TODO 3 — Το «aha»: 2ο batch = incremental

```python
# Προσγειώνονται 2 ΝΕΑ αρχεία (άλλες 4.000 γραμμές)
pdf.iloc[6000:8000].to_csv(...); pdf.iloc[8000:10000].to_csv(...)

run_autoloader()                            # ΞΑΝΑΤΡΕΧΕΙΣ τον ΙΔΙΟ κώδικα/checkpoint
# Bronze = 10.000  →  επεξεργάστηκε μόνο +4.000, ΟΧΙ ξανά τις 6.000
```

👉 **Εδώ γίνεται το «κλικ».** Ίδιος κώδικας, ίδιο checkpoint. Ο Auto Loader κοιτάζει το checkpoint,
βλέπει «τα πρώτα 2 αρχεία τα ξέρω», και διαβάζει **μόνο τα 2 νέα**.

| | Πριν | Μετά | Επεξεργάστηκε |
|---|---|---|---|
| Run 1 | 0 | 6.000 | 6.000 |
| Run 2 | 6.000 | 10.000 | **μόνο 4.000** |

> ⚠️ **Χρυσός κανόνας:** **ίδιο `checkpointLocation` σε όλα τα runs.** Αν το σβήσεις ή το αλλάξεις,
> η μνήμη χάνεται και ο Auto Loader ξαναδιαβάζει **τα πάντα** από την αρχή (duplicates!).

---

## 4. Πώς ξέρει ο Auto Loader ποια αρχεία είναι νέα;

Δεν συγκρίνει περιεχόμενα — κρατάει στο **checkpoint** μια λίστα με τα **ονόματα/paths** των
αρχείων που έχει ήδη επεξεργαστεί. Κάθε νέο run:
1. κάνει list τα αρχεία στο `LANDING`,
2. αφαιρεί όσα υπάρχουν ήδη στο checkpoint,
3. επεξεργάζεται **μόνο τα υπόλοιπα**,
4. προσθέτει τα νέα στο checkpoint.

Αυτό δίνει **exactly-once**: κάθε αρχείο μετράει **ακριβώς μία φορά**, ακόμα κι αν το pipeline
σταματήσει στη μέση και ξανατρέξει.

---

## 5. Σύγκριση: Μέρος 1 (watermark) vs Μέρος 2 (checkpoint)

| | **Μέρος 1 — Watermark** | **Μέρος 2 — Auto Loader / Checkpoint** |
|---|---|---|
| Ποιος θυμάται «πού έμεινα»; | **Εσύ** (σε ένα table) | Το **Databricks** (αυτόματα) |
| Πάνω σε τι; | γραμμές (π.χ. max id) | **αρχεία** στον φάκελο |
| Κώδικας | περισσότερος, χειροκίνητος | 2-3 γραμμές |
| exactly-once | πρέπει να το διασφαλίσεις | **ενσωματωμένο** |

Δύο τρόποι, **ίδια ιδέα**: «μη ξαναδιαβάζεις ό,τι έχεις ήδη».

---

## 6. Schema drift — όταν αλλάζει η μορφή των δεδομένων (TODO 4)

Στην πραγματικότητα τα exports **αλλάζουν χωρίς προειδοποίηση**. Σήμερα τα ΚΕΠ προσθέτουν στήλη
`priority` στο CSV. Τι γίνεται;

```python
batch3["priority"] = "HIGH"      # ⚠️ ΝΕΑ στήλη που δεν υπήρχε
batch3.to_csv(".../kep_batch3_newcol.csv")
run_autoloader()
# rescued = γραμμές με _rescued_data ≥ 100  →  το pipeline ΔΕΝ έσπασε
```

Χάρη στο `schemaEvolutionMode = "rescue"`, η άγνωστη στήλη **«διασώζεται»** μέσα στη στήλη
`_rescued_data` (σαν μικρό JSON) αντί να ρίξει σφάλμα.

### Οι 3 βασικές συμπεριφορές σε νέα στήλη (`schemaEvolutionMode`)
| Mode | Τι κάνει σε νέα στήλη |
|---|---|
| `rescue` (αυτό που χρησιμοποιούμε) | τη βάζει στο `_rescued_data`· **δεν σπάει**, δεν εξελίσσει το schema |
| `addNewColumns` (default) | σταματά **μία φορά** με σφάλμα, προσθέτει τη στήλη, και στο **επόμενο** run συνεχίζει |
| `failOnNewColumns` | σταματά με σφάλμα (πρέπει να επέμβεις χειροκίνητα) |
| `none` | αγνοεί νέες στήλες |

> **Γιατί `rescue` στο μάθημα;** Γιατί τρέχει σε **ένα πέρασμα** (δεν χρειάζεται restart), άρα
> είναι το πιο «φιλικό» για demo — και δείχνει καθαρά ότι **δεν χάνεις δεδομένα** όταν αλλάζει η μορφή.

---

## 7. Από Bronze σε Silver (TODO 5)

Από τα raw events χτίζεις ένα **Silver KPI table** ανά τύπο υπηρεσίας:
```python
spark.table(BRONZE).filter("request_id <= 10000")
    .groupBy("service_type")
    .agg(count("*").alias("total_requests"),
         round(avg("wait_time_minutes"),1).alias("avg_wait_min"))
# → 5 service types με πλήθος αιτημάτων & μέσο χρόνο αναμονής
```
Δείχνει ότι το Bronze (raw) γίνεται **business-ready** μέτρηση — η αξία βγαίνει στο Gold/Silver.

---

## 8. Self-check — τι περιμένεις να δεις

| Έλεγχος | Τιμή |
|---|---|
| Batch 1 → Bronze | **6.000** |
| Batch 2 (incremental) → Bronze | **10.000** (μόνο **+4.000**) |
| Audit column `_source_file` | υπάρχει |
| Batch 3 (νέα στήλη) → Bronze | **10.100** |
| Schema drift → `_rescued_data` | ≥ **100** γραμμές |
| Silver | **5** service types |

Αν όλα είναι `✅ OK` → πέτυχες.

---

## 9. Συχνά λάθη (για να τα προλάβεις στην τάξη)

- **Το stream «κολλάει» / τρέχει για πάντα** → ξέχασες `trigger(availableNow=True)` ή `awaitTermination()`.
- **Batch 2 ξαναδιάβασε ΟΛΑ τα αρχεία** → άλλαξες/έσβησες το checkpoint. Κράτα το **ίδιο** `CKPT`.
- **`checkpointLocation must be set`** → κάθε `writeStream` θέλει checkpoint.
- **Νέα στήλη έσπασε τη ροή** → λάθος `schemaEvolutionMode`. Με `rescue` δεν σπάει.

---

## 10. Αναλογία για την τάξη

> **Ο Auto Loader είναι ένας ταχυδρόμος.** Κάθε πρωί σου φέρνει **μόνο τα καινούρια γράμματα** —
> όχι όλη τη χρονιά από την αρχή. Το **checkpoint** είναι το σημάδι «μέχρι εδώ τα παρέλαβα».
> Κι αν έρθει γράμμα με παράξενο φάκελο (νέα στήλη), δεν το πετάει — το βάζει σε ένα **ειδικό
> συρτάρι** (`_rescued_data`) για να το δεις αργότερα.

---

## 11. Ερωτήσεις για να ανοίξεις συζήτηση (trainer)

1. *«Γιατί να μην ξαναδιαβάζω όλα τα αρχεία κάθε φορά;»* → κόστος, χρόνος, duplicates.
2. *«Τι θα γίνει αν σβήσω το checkpoint;»* → ξαναδιαβάζει τα πάντα → διπλά.
3. *«Τι κάνεις όταν αλλάξει το export και προσθέσουν στήλη στις 3 τα ξημερώματα;»* → schema drift / rescued.
4. *«Πού είναι η διαφορά με το watermark του Μέρους 1;»* → χειροκίνητο vs αυτόματο.

**Κλείσιμο:** *Το Μέρος 1 σου έδειξε το incremental «με το χέρι» (watermark). Το Μέρος 2 σου το
δίνει «δωρεάν, αξιόπιστα και ανθεκτικά σε αλλαγές» (Auto Loader + checkpoint).*
