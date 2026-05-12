# 🎓 Unity Catalog — Expert Labs (2-hour Workshop)

**Course:** Day 5 — Security & Governance
**Duration:** 120 minutes
**Audience:** Data Engineers, Data Stewards, Compliance Officers
**Difficulty:** ⭐⭐⭐⭐ Expert

## 🗓️ Workshop Timeline

| Slot | Lab | Duration | Mode |
|---|---|---|---|
| 0:00–0:30 | **Lab 1: Trainer Demo** | 30 min | Instructor leads, students observe |
| 0:30–1:30 | **Lab 2: Student Hands-On** | 60 min | Students execute autonomously |
| 1:30–2:00 | **Lab 3: Capstone Challenge** | 30 min | Students as Compliance Officer |

## 📦 Files

| Notebook | Steps | Role | Key concepts |
|---|---|---|---|
| [`01_Trainer_Demo.py`](01_Trainer_Demo.py) | 7 | Trainer | Catalog hierarchy, GRANT, mask, lineage, audit |
| [`02_Student_HandsOn.py`](02_Student_HandsOn.py) | 12 | Student | Personal schema, Bronze/Silver, masks, filters |
| [`03_Capstone_Challenge.py`](03_Capstone_Challenge.py) | 7 | Compliance Officer | PII discovery, audit, lineage, reporting |

## 📥 GitHub Raw URLs (για import σε Databricks)

```
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day5/Unity_Catalog_Labs/01_Trainer_Demo.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day5/Unity_Catalog_Labs/02_Student_HandsOn.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/refs/heads/main/Day5/Unity_Catalog_Labs/03_Capstone_Challenge.py
```

## 🎯 Learning Outcomes (μετά τα 3 labs)

Οι μαθητές θα μπορούν:

1. **Foundations**
   - Να εξηγήσουν το 3-level namespace του UC
   - Να δημιουργήσουν catalog/schema/volume με metadata
   - Να αναγνωρίσουν managed vs external objects

2. **RBAC & Permissions**
   - Να γράψουν GRANT/REVOKE statements
   - Να καταλάβουν την inheritance chain
   - Να εφαρμόσουν least-privilege principle

3. **Security Features**
   - Να γράψουν SQL functions για column masks
   - Να εφαρμόσουν row filters
   - Να χρησιμοποιήσουν dynamic views

4. **Lineage & Audit**
   - Να ερευνήσουν automatic lineage στο Catalog Explorer
   - Να γράψουν queries σε `system.access.audit`
   - Να κάνουν impact analysis με `system.access.column_lineage`

5. **Compliance**
   - Να εντοπίσουν PII columns στο workspace
   - Να συντάξουν GDPR compliance report
   - Να βρουν stale grants και permission gaps

## 🏗️ Prerequisites

### Workspace setup (πριν το workshop)
- [ ] Databricks Free Edition account (sufficient)
- [ ] Unity Catalog enabled (default σε νέα workspaces)
- [ ] Catalog `workspace` accessible
- [ ] SQL Warehouse ή Serverless compute διαθέσιμα
- [ ] Όλοι οι μαθητές έχουν user accounts (Entra ID sync)

### Trainer prep (1 ημέρα πριν)
- [ ] Run Lab 1 ολόκληρο σε δικό σου workspace (smoke test)
- [ ] Επιβεβαίωση ότι `system.access.*` tables είναι queryable
- [ ] Προετοιμασία list με `YOUR_NAME` για κάθε μαθητή (lower case)
- [ ] Backup: PDF screenshots των key outputs (αν spotι internet σπάσει)

## 🚀 Workshop Flow

### Opening (5 min) — πριν Lab 1
> «Σήμερα κάνουμε **εξειδικευμένο workshop** για Unity Catalog. 2 ώρες, 3 labs.
> Δεν χρειάζεται να γνωρίζετε advanced concepts — θα τα δούμε όλα live.
> Όλοι θα έχετε **personal schema** που σας ανήκει — δεν θα συγκρουστείτε
> με τους άλλους μαθητές.»

### Lab 1: Trainer Demo (30 min)
- **Mode**: Projector στο instructor laptop
- **Students**: ΔΕΝ εκτελούν τα ίδια κελιά — απλώς παρακολουθούν
- **Pacing**: 4 min ανά demo step + 2 min για Q&A
- **Cliffhangers**: «Στο επόμενο lab θα κάνετε αυτό μόνοι σας με δικό σας schema»

### Break (5 min) — μεταξύ Lab 1 και Lab 2

### Lab 2: Student Hands-On (60 min)
- **Mode**: Κάθε μαθητής στο δικό του Databricks tab
- **Critical first step**: Αλλαγή του `YOUR_NAME` variable στο πρώτο cell
- **Instructor role**: Περπατάτε στην αίθουσα, βοηθάτε ατομικά
- **Pacing target**: 5 min ανά step (12 steps × 5 min = 60 min)
- **Common stumbling blocks**:
  - Ξέχασαν να αλλάξουν `YOUR_NAME` → conflicts με άλλους
  - Permission errors → trainer βοηθά με GRANT
  - Mask δεν δουλεύει → check is_member() output

### Break (5 min) — πριν Lab 3

### Lab 3: Capstone Challenge (30 min)
- **Mode**: Individual ή pair-work (2-3 ατόμων)
- **Roleplay**: «Είσαι Compliance Officer — απάντησε 7 ερωτήσεις»
- **System tables warning**: Σε Free Edition κάποια system tables disabled
  → fallback queries υπάρχουν στο notebook
- **Wrap-up**: τελευταία 5 min — group discussion για findings

### Closing (5 min)
- Recap των 3 labs
- Q&A
- Links σε επόμενα steps:
  - Day 6 capstone (end-to-end pipeline με UC)
  - UC Best Practices guide
  - DLT pipeline με UC schema

## 🧯 Troubleshooting

### "Schema already exists" error
```
Cause: Δύο μαθητές χρησιμοποιούν ίδιο YOUR_NAME
Fix: Αλλάξτε σε `george2`, `george_pittas`, ή initials `gp`
```

### "system.access.audit not found"
```
Cause: Free Edition / system tables όχι enabled
Fix: Lab 3 has fallback queries — χρησιμοποιήστε `DESCRIBE HISTORY` αντί
```

### "Permission denied on CREATE SCHEMA"
```
Cause: User δεν έχει CREATE SCHEMA privilege
Fix: Trainer runs:
  GRANT CREATE SCHEMA ON CATALOG workspace TO `<user_email>`
```

### "Column mask εφαρμόστηκε αλλά βλέπω full ΑΦΜ"
```
Cause: Είστε admin → mask function δίνει full value σε admins
Expected behavior — όχι bug. Test ως non-admin για να δείτε masking effect.
```

### "ALTER COLUMN SET MASK fails"
```
Cause: Function path λάθος ή function δεν δημιουργήθηκε
Fix: 
  SHOW FUNCTIONS IN <your_schema>  -- verify function exists
  Use full path: workspace.aade_<name>.mask_afm
```

### "is_member() always returns FALSE"
```
Cause: Δεν έχετε join-άρει το group ακόμα
Note: Στο Free Edition, default groups: `account users`, `account admins`
Use these or create dummy logic με current_user() == 'specific_email'
```

## 📚 Συμπληρωματικά Resources

### Στο ίδιο repo
- **Original lab**: [`../Unity_Catalog_Notebook.py`](../Unity_Catalog_Notebook.py) (basic 7-step version)
- **Day 6 capstone**: [`../../Day6/AADE_DLT_Pipeline.py`](../../Day6/AADE_DLT_Pipeline.py) (UC στο DLT context)
- **Setup guide**: [`../../Day6/AADE_DLT_Setup_Guide.md`](../../Day6/AADE_DLT_Setup_Guide.md)

### Databricks docs
- [Unity Catalog Overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Manage privileges in Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/index.html)
- [Column masks](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters.html)
- [System tables](https://docs.databricks.com/admin/system-tables/index.html)

## 🎯 Post-Workshop Assessment

Optional quiz για students (κάθε ερώτηση 5 min):

1. Δώστε `SELECT` στο `aade_central` group σε όλα τα views του δικού σας schema
2. Δημιουργήστε column mask που εφαρμόζει IBAN masking (XX**-**1234)
3. Γράψτε query που βρίσκει όλα τα tables ΣΑΣ χωρίς data_owner tag
4. Εντοπίστε τα downstream objects μιας στήλης χρησιμοποιώντας Catalog Explorer

**Pass rate**: 3/4 → certified UC practitioner

---

> *«Governance δεν είναι εμπόδιο για velocity — είναι το πλαίσιο που σας αφήνει
> να τρέχετε γρήγορα χωρίς να σπάτε πράγματα.»*
