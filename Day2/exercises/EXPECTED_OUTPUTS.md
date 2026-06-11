# ✅ Expected Outputs — Άσκηση Ημέρα 2 (myDATA Pipeline)

Όλα τα νούμερα παρακάτω είναι **επαληθευμένα** πάνω στο πραγματικό `mydata_invoices_MESSY.csv`
(100 γραμμές). Αν τα δικά σας ταιριάζουν → σωστά.

---

## Μέρος 1 — Detection

**Bronze:** `Raw invoices: 100 rows`

**DQ Report — αναμενόμενα πλήθη:**

| # | Έλεγχος | Αναμενόμενο |
|---|---------|------------|
| [1] | NULL `issuer_afm` | **5** |
| [1] | NULL `vat_amount` | **2** |
| [2] | Duplicate `invoice_id` (διακριτά ids) | **3** → `INV-2025-000001`, `INV-2025-000067`, `INV-2025-000086` |
| [3] | Bad `issuer_afm` (όχι 9 ψηφία) | **4** |
| [4] | Negative `net_amount` | **3** |
| [5] | Future `issue_date` | **4** |
| [6] | Invalid `status` | **3** → `PENDING` (×2), `???` (×1) |
| [7] | Whitespace `issuer_name` | **5** |
| [8] | Bad date format (YYYY/MM/DD) | **3** → `2025/04/13`, `2025/04/15`, `2025/06/02` |
| [9] | NULL `vat_amount` | **2** |
| [10] | Orphan `receiver_afm` (εκτός master) | **11** γραμμές (4 διακριτά «ανύπαρκτα» ΑΦΜ: `101111111`, `999218972`, `999335615`, `999982277`) |

> ⚠️ **Προσοχή:** το παλιό demo-doc έλεγε «3 orphans» — **λάθος**. Το πραγματικό dataset έχει **11**.
> (3 διακριτά `999…` + το `101111111`, σε σύνολο 11 γραμμές.)

**Self-check τέλους:** όλα `✅ OK` και `🎉 Τέλος Μέρους 1!`

---

## Μέρος 2 — Cleanse & Quarantine

| Πίνακας | Αναμενόμενο πλήθος |
|---|---|
| `mydata_raw` (Bronze) | **100** |
| `mydata_quarantine` (critical rows) | **18** |
| `mydata_clean` (Silver, μετά dedup) | **90** |
| Αφαιρέθηκαν από dedup | **2** |

> Το Quarantine (18) > άθροισμα μεμονωμένων issues, γιατί μια γραμμή μπορεί να έχει **πολλά**
> προβλήματα ταυτόχρονα — το `OR` τα ενώνει χωρίς διπλομέτρηση.

**Self-check:** όλα `✅ OK`:
- Καμία NULL `issuer_afm` στο Silver (τα 9-ψήφια κρατήθηκαν, τα κακά → flagged αλλά οι null-afm γραμμές dropped).
- Κανένα αρνητικό `net_amount`.
- Κανένα διπλό `invoice_id`.
- Καμία NULL `vat_amount` (έγινε recompute από net × rate).

Τυπική γραμμή report: `Bronze 100 → Quarantine 18 + Silver 90`.

---

## Μέρος 3 — Enrich & Gold

- **Enriched rows == Silver rows == 90** (το `left` join **δεν** χάνει γραμμές — κρίσιμο σημείο).
- Νέες στήλες μετά το join: `official_name`, `sector`, `region`, `doy_name`.
- **Gold** = μία γραμμή ανά συνδυασμό `sector × region` (το ακριβές πλήθος εξαρτάται από
  ποιοι συνδυασμοί υπάρχουν στα 90 — τυπικά ~15-25 γραμμές).

**Gold στήλες:**
`sector, region, invoice_count, total_net_eur, total_vat_eur, total_with_vat_eur,
avg_invoice_eur, submitted, cancelled, pending, unknown_status`

**Top sectors (insight):** ο κλάδος με τον μεγαλύτερο `total_with_vat_eur` πρώτος.
(Τα ακριβή ευρώ εξαρτώνται από το τυχαίο dataset — κοιτάξτε σχετική κατάταξη, όχι απόλυτο νούμερο.)

**Before vs After:**
```
Bronze 100 → Quarantine 18 + Silver 90 → Gold (sector×region rows)
```

**Τελικό self-check:** όλα `✅ OK` και `🎉🎉 ΟΛΟΚΛΗΡΩΣΑΤΕ ΟΛΗ ΤΗΝ ΑΣΚΗΣΗ DAY 2!`

Στο schema `workspace.aade` πρέπει να υπάρχουν **4** πίνακες `mydata_*`:
`mydata_raw`, `mydata_quarantine`, `mydata_clean`, `mydata_gold`.
