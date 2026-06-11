# 📋 Step-by-Step — Άσκηση Ημέρα 2 (Fill-in-the-Blank Series)

**Οδηγός διευκόλυνσης** για trainer & εκπαιδευόμενο. Δείχνει *πώς να τρέξετε* τη σειρά,
δίνει **hints ανά TODO** (χωρίς να δίνει ολόκληρη τη λύση), και τα expected outputs.

> 4 notebooks · ~3.5 ώρες συνολικά · μπορούν να σπάσουν σε 2-3 sessions.
> Φιλοσοφία: **διαβάζω 🧠 ΕΝΝΟΙΑ → λύνω ✍️ TODO → ελέγχω self-check**.

---

## 🚀 SETUP (μία φορά, ~3')

Δεν χρειάζεται manual upload — το **Cell 0** κάθε notebook κατεβάζει τα CSV από GitHub.

- [x] Databricks workspace (Free Edition + **Serverless** ✅)
- [x] Νέο Notebook → Connect: Serverless
- [x] Import τα `Exercise*_STARTER.py` (Workspace → Import → file ή URL)
- [x] Τρέξτε **με τη σειρά**: 1 → 2 → 3 → 4 (το καθένα χτίζει το table του επόμενου)

> Αν δεν έχετε Unity Catalog (`workspace` catalog): δείτε `TROUBLESHOOTING.md` → Free Edition fallback.

---

## 🗺️ Η ροή με μια ματιά

```
MESSY.csv ─Μ1→ 🥉 mydata_raw ─Μ2→ 🚨 quarantine + 🥈 mydata_clean ─Μ3→ 🥇 mydata_gold
payments.csv ─────────────────── Μ4 (capstone, μόνοι σας) ──────────→ 🥇 payments_gold
```

---

# 🔵 ΜΕΡΟΣ 1 — Detection (~60')

**Στόχος:** Bronze ingest + εντοπισμός 10 κατηγοριών DQ issues. Δεν διορθώνουμε ακόμα — **μετράμε**.

| TODO | Τι ζητά | Hint (όχι λύση) |
|---|---|---|
| 1 | Read options | `"header"` + `"inferSchema"`, τιμή `"true"` |
| 2 | Audit metadata | `current_timestamp()` και `col("_metadata.file_path")` — **όχι** `input_file_name()` (UC) |
| 3 | NULL counts | μέσα στο `when(...)`: `col(c).isNull()` |
| 4 | Duplicates | `groupBy("invoice_id")` · όριο `> 1` |
| 5 | Bad AFM | regex `^\d{9}$` |
| 6 | Negatives | τελεστής `<` |
| 7 | Future dates | `>` `current_date()` |
| 8 | Bad status | μέθοδος `isin(...)` |
| 9 | Whitespace | συνάρτηση `trim(...)` |
| 10 | Bad date format | regex `^\d{4}-\d{2}-\d{2}$` |
| 11 | NULL vat | `isNull()` |
| 12 | Orphans | join type `left_anti` |

**✅ Expected:** Bronze=100· null afm=5· null vat=2· dups=3· bad afm=4· neg=3· future=4· bad status=3· whitespace=5· bad date=3· **orphans=11**.
Self-check → όλα `OK`, `🎉 Τέλος Μέρους 1!`

> 🧑‍🏫 **Trainer tip:** Σταθείτε στο TODO 1-2. Ρωτήστε: *«τι τύπο έδωσε ο Spark στο issuer_afm;»*
> Αν είναι `integer` → εκεί ζει η παγίδα leading-zero. Σύνδεση με την κουβέντα του πρωινού.

---

# 🟡 ΜΕΡΟΣ 2 — Cleanse & Quarantine (~70')

**Στόχος:** χωρισμός 🔴 critical (→ Quarantine) από 🟢🟡 fixable (→ Silver). Το «μεγάλο» μέρος.

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1a-1e | Flag εκφράσεις | `isNull()` · regex AFM · `< 0` · regex date · `~isin()` |
| 2 | Quarantine filter | ο τελεστής `|` (OR) — 4 φορές |
| 3a | Drop critical | `isNotNull()` (×2) |
| 3b | Normalize date | `regexp_replace(col, "/", "-")` |
| 3c | Trim | `trim(...)` |
| 3d | Bad AFM → NULL | `lit(None)` |
| 3e | Drop negatives | `>= 0` |
| 3f | Status fallback | `"UNKNOWN"` |
| 3g | Recompute vat | `col("vat_rate")` (net × rate) |
| 3h | Total | `+` |
| 4a-4c | Window dedup | `partitionBy("invoice_id")` · `.desc()` · `rn == 1` |

**✅ Expected:** Bronze 100 → **Quarantine 18** + **Silver 90** (2 αφαιρέθηκαν στο dedup).
Self-check: καμία null afm / negative / dup / null vat στο Silver.

> 🧑‍🏫 **Trainer tip:** Το «γιατί Quarantine και όχι σκέτο drop» είναι το κρίσιμο μάθημα.
> Ρωτήστε: *«σε έναν μήνα ο προϊστάμενος ρωτά γιατί λείπουν 12 τιμολόγια — τι απαντάτε;»*
> Το Window dedup (TODO 4) αξίζει whiteboard: partition → order → row_number → rn==1.

---

# 🟢 ΜΕΡΟΣ 3 — Enrich & Gold (~60')

**Στόχος:** join με master (ονόματα/κλάδοι/ΔΟΥ) → aggregation για Business.

| TODO | Τι ζητά | Hint |
|---|---|---|
| 1a | Alias join key | `"issuer_afm"` |
| 2a-2c | Joins | `on="issuer_afm"` · `how="left"` (×2) |
| 3a | groupBy | `"sector", "region"` |
| 3b-3c | Conditional counts | `"Ακυρωμένο"` · `"Εκκρεμές"` |
| 3d | Sort | `desc(...)` |
| 4a-4b | Delta write | `"delta"` · `"overwrite"` |
| 5a | Top sectors | `groupBy("sector")` |
| 6a-6c | Table names | `mydata_raw` · `mydata_quarantine` · `mydata_clean` |

**✅ Expected:** Enriched == Silver (=90· left join δεν χάνει)· Gold = 1 γραμμή ανά `sector × region`.
4 tables `mydata_*` στο schema. Self-check → `🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ!`

> 🧑‍🏫 **Trainer tip:** Το `inner` vs `left` είναι το κλειδί. Δείξτε τι θα γινόταν με `inner`:
> τα flagged-to-NULL AFM θα **εξαφανίζονταν**. Ρωτήστε *«θέλουμε να χάσουμε τιμολόγια;»* → όχι → `left`.

---

# 🏆 ΜΕΡΟΣ 4 — Payments Capstone (Bonus, ~45')

**Στόχος:** **χωρίς καθοδήγηση** — εφαρμόζουν μόνοι τους ΟΛΟ το pipeline σε νέο dataset.

- Νέο: τα status είναι **Αγγλικά** (`Confirmed/Pending/Failed`)· `'pending '` & `'??'` → `UNKNOWN`.
- Gold ανά `payment_method × region` + **failure rate** insight.

**✅ Expected:** Raw 250 → Clean 240 → **Dedup 235**. Self-check → `🏆 CAPSTONE ΟΛΟΚΛΗΡΩΘΗΚΕ!`

> 🧑‍🏫 **Trainer tip:** Αφήστε τους 30' μόνους τους πριν δείξετε λύση. Είναι το «test» της ημέρας.

---

## 🧹 Reset (αν θέλουν να ξαναρχίσουν)

```python
for t in ["mydata_raw","mydata_quarantine","mydata_clean","mydata_gold","payments_gold"]:
    spark.sql(f"DROP TABLE IF EXISTS workspace.aade.{t}")
```

## 🎯 Learning outcomes (τι ξέρουν στο τέλος)

read options · inferSchema παγίδα · audit metadata (UC `_metadata`) · NULL-count idiom ·
`rlike`/`isin`/`left_anti` · quarantine pattern · `when().otherwise()` · `regexp_replace`/`trim` ·
recompute · **Window dedup** · `left` vs `inner` join · `broadcast` · groupBy/agg ·
conditional aggregation · Delta write modes. → Όλη η «Ημέρα 2: Quality & Transformations».
