# 🔁 Άσκηση Ημέρα 4 — Full Load vs Incremental Load

> **Ρόλος 3: Μηχανικοί Δεδομένων · ΑΑΔΕ** · Fill-in-the-Blank σειρά 4 μερών
> **~4.5 ώρες · ~58 TODOs** · Dataset: `kep_requests.csv` (10.000 αιτήματα ΚΕΠ)

---

## 🎯 Γιατί υπάρχει αυτή η άσκηση (η μεγάλη εικόνα)

Στο πανεπιστήμιο μαθαίνεις να **φορτώνεις ένα αρχείο**. Στην παραγωγή, τα δεδομένα **δεν
έρχονται μία φορά — έρχονται συνέχεια**: τα ΚΕΠ ρίχνουν νέα αιτήματα κάθε μέρα, το TAXIS κάθε
βράδυ, το myDATA συνεχώς. Η πραγματική δουλειά ενός Data Engineer είναι να απαντήσει:

> **«Ήρθαν νέα δεδομένα. Τα ξαναφορτώνω ΟΛΑ από την αρχή (full load), ή μόνο ό,τι ΑΛΛΑΞΕ (incremental);»**

Η απόφαση αυτή καθορίζει **κόστος, ταχύτητα και ορθότητα** κάθε pipeline. Αυτή η άσκηση σε
μαθαίνει και τις δύο στρατηγικές, και τα production εργαλεία που κάνουν το incremental εύκολο
και αξιόπιστο: **watermark, Auto Loader, Structured Streaming, SCD2**.

### Γιατί έχει σημασία στην πράξη
| | Full load | Incremental |
|---|---|---|
| Επεξεργάζεται | **όλα** κάθε φορά | **μόνο τα νέα/αλλαγμένα** |
| Κόστος/χρόνος | μεγάλο (σπατάλη) | μικρό |
| Κίνδυνος | duplicates αν δεν προσέξεις | πρέπει να ξέρεις «πού έμεινα» |
| Πότε το χρησιμοποιώ | μικρά / dimension tables | μεγάλα fact tables, συνεχής ροή |

**Ο κανόνας:** full load όταν είναι μικρό & απλό· incremental όταν μετράει το κόστος/χρόνος.

---

## 📖 Το Σενάριο

Τα **ΚΕΠ** στέλνουν στην ΑΑΔΕ τα αιτήματα πολιτών (πιστοποιητικά, ανανεώσεις ταυτότητας, άδειες
κ.λπ.). Κάθε αίτημα έχει: ποιος το έκανε, πότε, τι τύπος, χρόνος αναμονής, αποτέλεσμα ελέγχου.
Κάθε μέρα έρχονται **νέα** αιτήματα και ενημερώνονται **υπάρχοντα** (π.χ. αλλάζει το αποτέλεσμα
ελέγχου). Θα χτίσεις pipelines που τα φέρνουν σωστά, φθηνά και χωρίς διπλά.

---

## 🗺️ Τι θα χτίσεις (4 Μέρη, ίδιο νήμα)

```
kep_requests.csv  (10.000 αιτήματα ΚΕΠ)
   │
 Μέρος 1 ──► Full load (overwrite, ΟΛΑ)  vs  Incremental (watermark, ΜΟΝΟ τα νέα)  + audit log
   │
 Μέρος 2 ──► Auto Loader: το checkpoint κρατάει μόνο του «ποια αρχεία διάβασα» + schema drift
   │
 Μέρος 3 ──► Structured Streaming: foreachBatch (DQ/quarantine → dedup → MERGE → Gold) exactly-once
   │
 Μέρος 4 ──► SCD Type 2: incremental ΜΕ ιστορικό (κρατάμε κάθε version μιας εγγραφής)
```

---

## 📚 Τα 4 Μέρη — τι κάνει & γιατί έχει σημασία

### 🔵 Μέρος 1 — Full vs Incremental (`Exercise1_FullVsIncremental_STARTER.py`, ~80')
**Τι κάνεις:** Φορτώνεις τα δεδομένα με τρεις τρόπους και τους **μετράς** με ένα audit log:
1. **Full load** — overwrite όλου του target (απλό, αλλά επεξεργάζεται 10.000 κάθε φορά).
2. **Incremental append (watermark)** — κρατάς το «μέχρι ποιο id έφτασα» και φέρνεις **μόνο τα νέα** (2.000).
3. **Incremental upsert (MERGE)** — διαχειρίζεσαι και **αλλαγές** σε παλιές εγγραφές, όχι μόνο νέες.

**Η σημασία:** Εδώ «κλικάρει» η διαφορά κόστους — το audit log δείχνει μαύρο-άσπρο ότι το
incremental έκανε **~80% λιγότερη δουλειά**. Μαθαίνεις και το **watermark** (το «πού έμεινα»).

### 🟡 Μέρος 2 — Auto Loader (`Exercise2_AutoLoader_STARTER.py`, ~80')
**Τι κάνεις:** Τα ΚΕΠ ρίχνουν **αρχεία** σε έναν φάκελο. Ο **Auto Loader** διαβάζει αυτόματα
**μόνο τα νέα**, χάρη στο **checkpoint** (η «μνήμη» του τι έχει ήδη διαβαστεί). Ρίχνεις 2ο batch
αρχείων → ξανατρέχεις → επεξεργάζεται **μόνο +4.000**, όχι ξανά τις 6.000.

**Η σημασία:** Δεν χρειάζεται να κρατάς εσύ watermark — το Databricks το κάνει για σένα.
Επίσης μαθαίνεις **schema drift**: όταν έρθει αρχείο με **νέα στήλη**, το pipeline **δεν σπάει** —
η νέα στήλη «διασώζεται» στο `_rescued_data`.

### 🟢 Μέρος 3 — Structured Streaming (`Exercise3_Streaming_Merge_STARTER.py`, ~75')
**Τι κάνεις:** Το ίδιο μοντέλο, αλλά για **συνεχή ροή**. Γράφεις **εσύ** όλο τον επεξεργαστή
micro-batch (`foreachBatch`) που κάνει **4** πράγματα: (1) **Data Quality** — οι «κακές» γραμμές
πάνε σε **quarantine** (δεν μολύνουν το Silver), (2) **dedup** (κρατά την τελευταία version),
(3) **MERGE** upsert στο Silver, (4) **Gold** KPIs (με `pct_flagged`) + batch metrics. Τρέχεις 3
φορές για να δεις το **exactly-once** (re-run χωρίς νέα → τίποτα δεν διπλογράφεται).

**Η σημασία:** Είναι το production pattern για streaming pipelines — «μία γραμμή κώδικα ροής,
μέσα κανονική batch λογική (MERGE)».

### 🏆 Μέρος 4 — SCD Type 2 (`Exercise4_SCD2_Bonus_STARTER.py`, ~60', bonus)
**Τι κάνεις:** Ο upsert κρατά μόνο την **τρέχουσα** τιμή. Το **SCD Type 2** κρατά **κάθε version**
με `valid_from` / `valid_to` / `is_current` / `version`. Έτσι απαντάς: *«ποιο ήταν το αποτέλεσμα
του αιτήματος **τον Μάρτιο**;»*. Τρέχεις 2 ημέρες αλλαγών ώστε μια εγγραφή να φτάσει 3 versions.

**Η σημασία:** Το πιο εξελιγμένο incremental pattern — απαραίτητο για **audit & compliance** σε
δημόσιο φορέα (πρέπει να ξέρεις τι ίσχυε σε κάθε χρονική στιγμή).

---

## 🧠 Βασικές έννοιες (με απλές αναλογίες)

- **Full vs Incremental** — *Διαβάζεις όλη τη χρονιά γραμμάτων κάθε πρωί, ή μόνο τα σημερινά;*
- **DataFrame vs Delta table** — Το **DataFrame** είναι ο υπολογισμός στη μνήμη (προσωρινό). Το
  **Delta table** είναι τα αποθηκευμένα δεδομένα στον δίσκο (μόνιμο). *«Το DataFrame είναι αυτό
  που υπολογίζεις· το Delta table αυτό που κρατάς.»*
- **Watermark** — ο **σελιδοδείκτης** «μέχρι εδώ έχω φορτώσει» (το κρατάς εσύ σε ένα table).
- **Checkpoint** — ο **αυτόματος σελιδοδείκτης** του Auto Loader/streaming («ποια αρχεία διάβασα»).
- **MERGE / upsert** — *«αν υπάρχει → ενημέρωσέ το· αν όχι → πρόσθεσέ το»* (incremental χωρίς διπλά).
- **Auto Loader (`cloudFiles`)** — ο **ταχυδρόμος** που φέρνει αυτόματα μόνο τα νέα αρχεία.
- **Rescued data** — όταν έρθει αρχείο με μη-αναμενόμενη στήλη, αντί να «σπάσει», τη **διασώζει**.
- **foreachBatch** — τρέχεις **batch** λογική (MERGE) μέσα σε **stream**.
- **Exactly-once** — κάθε εγγραφή επεξεργάζεται **ακριβώς μία φορά** (χάρη στο checkpoint).
- **SCD Type 2** — κρατάμε **ιστορικό** εκδόσεων κάθε εγγραφής, όχι μόνο την τελευταία.

---

## ▶️ Πώς το τρέχεις

1. **Databricks** workspace (Free Edition + **Serverless** — υποστηρίζει Auto Loader & streaming).
2. Import κάθε `Exercise*_STARTER.py` ως notebook.
3. **Τρέξε τα Μέρη με τη σειρά** (1 → 2 → 3 → 4). Το **Cell 0** κατεβάζει μόνο του το CSV.
4. Σε κάθε `# TODO`, συμπλήρωσε τα `_____`. Πάνω από κάθε TODO υπάρχει κελί **🧠 ΕΝΝΟΙΑ** που
   εξηγεί *τι / γιατί / πώς* — **διάβασέ το πρώτα**.
5. **Streaming cells (Μέρη 2 & 3):** άσε τα να **ολοκληρωθούν** (έχουν `awaitTermination`) — δεν
   μένουν «running» για πάντα.
6. Κάθε Μέρος κλείνει με **self-check** (`✅ OK / ❌ FAIL`). Στόχος: όλα OK.

> Κόλλησες; Δες το αντίστοιχο `_SOLUTION.py` (πλήρης σχολιασμένη λύση), τα `EXPECTED_OUTPUTS.md`
> (αναμενόμενα νούμερα) και το `TROUBLESHOOTING.md`. Hints ανά TODO: `STEP_BY_STEP_Exercises.md`.

---

## ✅ Τι ξέρεις στο τέλος

full vs incremental · audit log/metrics · watermark · append vs MERGE upsert · **Auto Loader**
(checkpoint, schema drift, rescued data) · **Structured Streaming** (foreachBatch: DQ/quarantine,
dedup, MERGE, exactly-once) · **SCD Type 2**. Δηλαδή: πώς να φορτώνεις **μόνο ό,τι άλλαξε — αξιόπιστα, φθηνά,
χωρίς διπλά**. Αυτή είναι η καθημερινή δουλειά σε ένα production lakehouse.

---

## 🔗 Σχέση με την Ημέρα 3

Η **Ημέρα 3** έδειξε το incremental **μέσα στο Delta** (MERGE, Change Data Feed, time travel).
Η **Ημέρα 4** το πάει στο **ingestion** (full vs incremental, Auto Loader, streaming, SCD2).
Μαζί καλύπτουν ένα **6ωρο+ Day 3+4** με κοινό νήμα: **incremental data processing**.

---

## 📂 Αρχεία

| Αρχείο | Θέμα | Διάρκεια | TODOs |
|---|---|---|---|
| `Exercise1_FullVsIncremental_STARTER.py` | Full vs incremental + audit log + reconciliation | ~80' | ~16 |
| `Exercise2_AutoLoader_STARTER.py` | Auto Loader + schema drift + Silver agg | ~80' | ~15 |
| `Exercise3_Streaming_Merge_STARTER.py` | Streaming foreachBatch (DQ/quarantine + dedup + MERGE + Gold) | ~75' | ~15 |
| `Exercise4_SCD2_Bonus_STARTER.py` | SCD Type 2 (versioned history) | ~60' | ~12 |
| `*_SOLUTION.py` | Πλήρεις, σχολιασμένες λύσεις | — | — |
| `STEP_BY_STEP_Exercises.md` | Οδηγός + hints ανά TODO + trainer tips | — | — |
| `EXPECTED_OUTPUTS.md` | Αναμενόμενα αποτελέσματα (επαληθευμένα) | — | — |
| `TROUBLESHOOTING.md` | Συχνά σφάλματα & λύσεις | — | — |

➡️ Ξεκίνα από `Exercise1_FullVsIncremental_STARTER.py`.
