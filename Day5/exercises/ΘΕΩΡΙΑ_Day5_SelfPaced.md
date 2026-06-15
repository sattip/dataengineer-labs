# 📚 Θεωρία Ημέρας 5 — Performance & Security (Self-Paced)

> Οδηγός **αυτο-μελέτης**. Διάβασε την ενότητα κάθε Μέρους **πριν** ανοίξεις το αντίστοιχο
> `Exercise*_STARTER.py`. Στο τέλος κάθε ενότητας υπάρχει «🤔 Έλεγχος κατανόησης» — απάντησε
> νοερά πριν προχωρήσεις. Δεν χρειάζεσαι εισηγητή: θεωρία εδώ → πράξη στο notebook → self-check.

---

## 🧭 Πώς δουλεύει το Spark (το mental model που χρειάζεσαι παντού)

Πριν μιλήσουμε για performance, κατάλαβε **πώς εκτελείται** ένα query:

1. Γράφεις **transformations** (`filter`, `join`, `groupBy`). Είναι **lazy** — δεν τρέχει τίποτα.
2. Μια **action** (`count`, `write`, `collect`) πυροδοτεί εκτέλεση.
3. Ο Spark σπάει τη δουλειά σε **stages**· κάθε stage σε **tasks** (1 task ανά **partition**).
4. Tasks τρέχουν **παράλληλα** στους workers. Άρα: **partitions = ο βαθμός παραλληλισμού**.
5. Όταν χρειάζεται αναδιανομή δεδομένων μεταξύ partitions (π.χ. `groupBy`, join), γίνεται
   **shuffle** — δεδομένα ταξιδεύουν στο δίκτυο. **Το shuffle είναι ό,τι πιο ακριβό υπάρχει.**

> 🔑 **Όλη η performance tuning** = «κάνε λιγότερο/φθηνότερο shuffle & διάβασε λιγότερα δεδομένα».

### Πώς διαβάζεις ένα query plan (`explain`)
- `df.explain(mode="formatted")` → δείχνει το **physical plan**.
- **`Exchange`** = shuffle (ακριβό· θες να τα ελαχιστοποιείς).
- **`BroadcastHashJoin`** = γρήγορος join χωρίς shuffle του μεγάλου table (καλό!).
- **`SortMergeJoin`** = ο default join με shuffle & στα δύο (ακριβός σε μεγάλα).
- **`PartitionFilters` / `PushedFilters`** = το engine πετάει δεδομένα νωρίς (data skipping/pruning).

> ⚠️ **Serverless:** το `df.rdd...` και το `df._jdf...` ΔΕΝ επιτρέπονται. Μέτρα partitions με
> `spark_partition_id()` και πιάσε το plan με `explain()` (το βλέπεις στα helpers των ασκήσεων).

---

## 🔵 Μέρος 1 — Partitioning, Joins & Caching

### Θεωρία
**Partitions:** Πολλά μικρά partitions → overhead (πολλά tasks). Λίγα τεράστια → κακός παραλληλισμός
& OOM. Στόχος: partitions ~100-200MB το καθένα.
- **`repartition(n)`** → ακριβώς `n` partitions, **full shuffle** (ισορροπημένα αλλά ακριβό). Χρήση:
  πριν από μεγάλο write ή για να σπάσεις skew.
- **`coalesce(n)`** → **μειώνει** partitions **χωρίς** full shuffle (φθηνό). Χρήση: να μαζέψεις
  πολλά μικρά partitions μετά από φιλτράρισμα. Δεν μπορεί να **αυξήσει** αποδοτικά.

**Join strategies:**
- **Broadcast join:** αν το ένα table είναι **μικρό** (< ~10-30MB), ο Spark το στέλνει ολόκληρο
  σε κάθε worker → **κανένα shuffle** του μεγάλου. Το ζητάς ρητά με `broadcast(small_df)`.
  Είναι ο #1 τρόπος επιτάχυνσης για «fact ⨝ dimension».
- **Sort-Merge Join (default):** shuffle & sort και τα δύο tables στο join key. Ακριβό για μεγάλα.

**Caching / Materialization:** Αν ξαναχρησιμοποιείς ένα ακριβό αποτέλεσμα:
- Σε **classic cluster**: `df.cache()` (μένει στη μνήμη).
- Σε **Serverless**: το `.cache()`/`.persist()` **ΔΕΝ υποστηρίζεται** (auto-caching). Ο σωστός
  τρόπος = **materialize σε Delta table** (`write.saveAsTable`) και διάβασε από εκεί.

**Partitioned write (`partitionBy`):** χωρίζει το table σε φακέλους ανά τιμή στήλης. Query με
`WHERE region='Αττική'` διαβάζει **μόνο** εκείνον τον φάκελο (**partition pruning**). ⚠️ Καλό **μόνο**
για low-cardinality στήλες (π.χ. region, year) — όχι για ΑΦΜ (θα φτιάξεις εκατομμύρια φακέλους!).

> 🤔 **Έλεγχος κατανόησης:** Γιατί το broadcast join είναι γρήγορο; Πότε NA ΜΗΝ κάνεις
> `partitionBy(afm)`; Ποια η διαφορά repartition vs coalesce;

---

## 🧨 Μέρος 2 — Data Skew

### Θεωρία
**Skew** = ένα κλειδί έχει δυσανάλογα πολλές γραμμές. Όταν γίνεται shuffle με αυτό το κλειδί,
**όλες** οι γραμμές του πάνε στο **ίδιο partition → ένα task** κάνει το 90% της δουλειάς ενώ τα
άλλα τελειώνουν αμέσως. Το job «κολλάει στο 99%».

**Εντοπισμός:** `groupBy(key).count()` → αν το max είναι **τάξεις μεγέθους** πάνω από τον μέσο όρο
(**skew ratio = max/avg**), έχεις skew. Δες και τα μεγέθη partitions με `spark_partition_id()`.

**Λύση — Salting:** προσθέτεις ένα ψευδο-τυχαίο «αλάτι» (`salt = id % N`) στο κλειδί. Έτσι το καυτό
κλειδί σπάει σε **N κουβάδες** που μοιράζονται σε N tasks. Για **aggregation** το κάνεις σε **δύο
στάδια**: (1) `groupBy(key, salt)` → μερικά αθροίσματα, (2) `groupBy(key)` πάνω στα μερικά → τελικό.
**Κρίσιμο:** το salting αλλάζει τη **διανομή**, ΟΧΙ το **αποτέλεσμα** (το self-check το αποδεικνύει).

**AQE (Adaptive Query Execution):** ενεργό by default· εντοπίζει & σπάει skewed partitions **αυτόματα
σε joins**. Το manual salting το χρειάζεσαι για βαριά aggregations ή όταν το AQE δεν αρκεί.

> 🤔 **Έλεγχος:** Γιατί ένα καυτό κλειδί κάνει ένα task αργό; Πώς το salting μοιράζει τη δουλειά
> χωρίς να αλλάζει το άθροισμα;

---

## 🔐 Μέρος 3 — Masking & Row-Level Security

### Θεωρία
Σε δημόσιο φορέα, τα **PII** (ΑΦΜ, εισοδήματα) δεν τα βλέπουν όλοι. Τρία επίπεδα προστασίας:
- **Column masking:** κρύβεις/αλλοιώνεις **τιμές στήλης** (π.χ. ΑΦΜ → `***666`, ποσό → NULL).
- **Row-level security (RLS):** κρύβεις **ολόκληρες γραμμές** (π.χ. ο analyst Αττικής βλέπει μόνο Αττική).
- **`is_account_group_member('group')`:** η συνάρτηση-κλειδί — επιστρέφει `true/false` ανάλογα με το
  group του **τρέχοντος** χρήστη. Πάνω της χτίζεις «ποιος βλέπει τι».

**Δύο τρόποι υλοποίησης:**
1. **Dynamic view** (παντού): `CASE WHEN is_account_group_member(...) THEN raw ELSE masked END`.
   Πλήρως demonstrable, τρέχει σε serverless.
2. **UC policies** (production): `ALTER TABLE ... SET MASK fn` / `SET ROW FILTER fn ON (col)` — η
   πολιτική κολλάει στο **ίδιο το table**, οπότε ισχύει σε **κάθε** query (όχι μόνο σε views).

> 🤔 **Έλεγχος:** Ποια η διαφορά masking vs RLS; Γιατί ένα UC column mask είναι ασφαλέστερο από view;

---

## 🏛️ Μέρος 4 — Governance & Audit

### Θεωρία
**Unity Catalog** = ο κεντρικός governance layer. Βασικά:
- **GRANT/REVOKE:** για ανάγνωση table χρειάζονται **δύο**: `GRANT USE SCHEMA` (να «δει» το schema)
  + `GRANT SELECT` (να διαβάσει). ⚠️ `USE SCHEMA`, **όχι** το legacy `USAGE`. **Least privilege:**
  δίνεις μόνο ό,τι χρειάζεται — μικρότερο **blast radius** σε compromise.
- **`information_schema`:** κάθε catalog έχει metadata (tables/columns/grants). Το χρησιμοποιείς για
  **προγραμματιστικό inventory** & **PII discovery** (ψάχνεις column names όπως `afm`, `amka`, `email`).
  Για GDPR πρέπει να ξέρεις **πού** ζει το PII — αυτό το query το βρίσκει αυτόματα.
- **System tables (`system.access.audit`):** **audit trail** — ποιος έκανε query, πότε, σε ποιο
  table. Απαντά «ποιος είδε τα εισοδήματα τον Μάρτιο;». (Σε Free Edition ίσως ανενεργά.)

> 🤔 **Έλεγχος:** Γιατί χρειάζονται 2 grants για ένα SELECT; Πώς βρίσκεις όλο το PII χωρίς να ψάξεις στο χέρι;

---

## 🧊 Μέρος 5 — Liquid Clustering & Data Skipping (Advanced)

### Θεωρία
**Πρόβλημα του partitioning:** άκαμπτοι φάκελοι· κακό για high-cardinality· over/under-partitioning·
δεν αλλάζει εύκολα. **Liquid Clustering (`CLUSTER BY`)** το λύνει:
- Ομαδοποιεί «κοντινά» δεδομένα σε files **δυναμικά & πολυδιάστατα** (όχι σταθεροί φάκελοι).
- **Αλλάζεις clustering keys** με `ALTER TABLE ... CLUSTER BY` **χωρίς rewrite**.
- Καλό και για high-cardinality (π.χ. ΑΦΜ), όπου το `partitionBy` θα κατέρρεε.
- Το clustering υλοποιείται/συντηρείται με **`OPTIMIZE`**.

**Data skipping:** Το Delta κρατά **min/max stats** ανά file. Ένα `WHERE region_id=3` διαβάζει **μόνο**
τα files που *μπορεί* να περιέχουν 3 → λιγότερο I/O. Το clustering κάνει αυτά τα stats «σφιχτά»
(τα όμοια μαζί), άρα πιο αποτελεσματικό skipping.

**Deletion Vectors:** Κανονικά ένα `DELETE`/`UPDATE` ξαναγράφει ολόκληρα files. Με DV, το Delta
**σημειώνει** ποιες γραμμές διαγράφηκαν (merge-on-read) → πολύ ταχύτερα, χωρίς rewrite.

**Liquid Clustering vs Z-ORDER (Ημέρα 3):** το Z-ORDER είναι παλαιότερο, απαιτεί επανάληψη σε κάθε
OPTIMIZE και δεν αλλάζει εύκολα· το Liquid Clustering είναι ο σύγχρονος αντικαταστάτης.

> 🤔 **Έλεγχος:** Πότε Liquid Clustering αντί για `partitionBy`; Τι κάνει το OPTIMIZE σε clustered
> table; Γιατί τα Deletion Vectors είναι ταχύτερα;

---

## 🔏 Μέρος 6 — PII Tokenization & ABAC (Advanced)

### Θεωρία
**Τρεις βαθμίδες προστασίας ΑΦΜ — μη τις μπερδεύεις:**
| Τεχνική | Τι κάνει | Joinable; | Αναστρέψιμο; |
|---|---|---|---|
| **Masking** (Μέρος 3) | κρύβει στον non-cleared (`***`) | ❌ | (δεν χρειάζεται) |
| **Pseudonymization** (hash) | ΑΦΜ → σταθερό token | ✅ (ίδιο ΑΦΜ→ίδιο token) | ❌ |
| **Tokenization (vault)** | token ↔ ΑΦΜ σε ασφαλή πίνακα | ✅ | ✅ (με το vault) |

**`sha2(afm, 256)`** → 64-hex token, **ντετερμινιστικό** (joinable μεταξύ datasets) αλλά
**μη-αναστρέψιμο**. Ιδανικό για να **μοιραστείς** δεδομένα χωρίς να εκθέσεις ΑΦΜ.

**Salted hashing:** σκέτο `sha2(afm)` σπάει με rainbow tables (λίστα ΑΦΜ → hashes). Με **salt**
(μυστικό prefix) `sha2(concat(salt, afm))` τα tokens γίνονται αδύνατο να αντιστραφούν χωρίς το salt,
και **διαφορετικά ανά dataset** → αποτρέπει cross-dataset linkage. (Το salt → secret scope, ΟΧΙ στον κώδικα.)

**ABAC (Attribute-Based Access Control):** αντί για hardcoded κανόνες, φτιάχνεις πίνακα
**`entitlements`** (`user_email → allowed_region`) και φιλτράρεις με **`current_user()`**. Κάθε χρήστης
βλέπει **δυναμικά** μόνο τα δικά του rows. Προσθέτεις/αφαιρείς δικαιώματα = εγγραφές στον πίνακα,
**χωρίς να αλλάξεις κώδικα/views**. Αυτός είναι ο production τρόπος για RLS σε κλίμακα.

**Sensitivity tags:** μαρκάρεις στήλες (`sensitivity = PII`) ώστε εργαλεία discovery/governance να τις
βρίσκουν αυτόματα.

> 🤔 **Έλεγχος:** Διαφορά masking vs pseudonymization; Γιατί salt; Γιατί ABAC με πίνακα αντί για
> hardcoded `region='Αττική'`;

---

## 📖 Glossary (γρήγορη αναφορά)

- **Partition** — κομμάτι δεδομένων που τρέχει ως ένα task (μονάδα παραλληλισμού).
- **Shuffle** — αναδιανομή δεδομένων μεταξύ partitions μέσω δικτύου (ακριβό).
- **Broadcast** — αποστολή μικρού table σε όλους τους workers (αποφυγή shuffle).
- **Skew** — άνιση κατανομή κλειδιού → ένα task υπερφορτωμένο.
- **Salting** — σπάσιμο καυτού κλειδιού σε N κουβάδες.
- **AQE** — Adaptive Query Execution (auto-optimization στο runtime).
- **Data skipping** — παράλειψη files βάσει min/max stats.
- **Liquid Clustering** — δυναμικό multi-dim clustering (`CLUSTER BY`).
- **Deletion Vectors** — soft deletes χωρίς rewrite.
- **Masking** — αλλοίωση τιμών στήλης.
- **RLS** — Row-Level Security (φιλτράρισμα γραμμών ανά χρήστη).
- **Pseudonymization** — μη-αναστρέψιμο, joinable token (sha2).
- **ABAC** — δικαιώματα βάσει attributes/entitlements + `current_user()`.
- **information_schema / system tables** — metadata & audit.

---

✅ Όταν καταλάβεις μια ενότητα → άνοιξε το αντίστοιχο `Exercise*_STARTER.py`, λύσε τα `_____`,
και επιβεβαίωσε με το self-check. Κόλλησες; → `STEP_BY_STEP_Exercises.md` (hints) ή το `_SOLUTION.py`.
