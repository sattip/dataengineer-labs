# 🚀 Databricks Workspace Setup — Trainee Guide

Πλήρες step-by-step για να στήσει ο κάθε trainee το Databricks Free Edition workspace του ώστε να τρέξει **όλα τα labs** (Day 2 + Day 3) χωρίς προβλήματα.

> **Συστήνεται**: Διαβάστε αυτόν τον οδηγό **πριν** το πρώτο live class. ~10 λεπτά setup time.

---

## 📋 Συνολική εικόνα

Στο Databricks workspace θα έχετε:

```
Catalog: workspace
└── Schema: aade
    ├── Volume: aade_data            ← master CSVs (Day 2 + Day 3)
    ├── Volume: mydata_raw           ← μόνο για Lab 3 myDATA
    └── Tables (auto-created από notebooks):
        ├── tax_declarations_silver
        ├── tax_declarations_quarantine
        ├── data_contract_audit
        ├── mydata_raw / clean / quarantine / gold
        └── ...
```

```
Workspace folders (αριστερό sidebar):
└── Users / <your-email> /
    └── data-engineer-training /
        ├── Day2 /
        │   ├── Lab1_LIVEDEMO_FreeEdition
        │   ├── Lab2_EXERCISE2_Payments
        │   └── Lab3_MYDATA_CLEANUP_Demo
        └── Day3 /
            ├── Άσκηση_3_DataContract
            └── Άσκηση_4_Lineage_History
```

---

## Βήμα 1 — Login στο Databricks Free Edition

1. Πήγαινε στο https://www.databricks.com/learn/free-edition
2. Click **Get started for free** → δημιουργία account (αν δεν έχεις)
3. Login → επιλογή region (κλείσε τον πιο κοντινό — π.χ. EU West)
4. Περίμενε ~2 min για provisioning του workspace

**Επιβεβαίωση**: βλέπεις αριστερά sidebar με: Workspace, Catalog, SQL, Compute, Jobs, ML.

---

## Βήμα 2 — Δημιουργία Schema + Volume

**Option A — Αυτόματη (συστήνεται)**: Αφήστε το πρώτο notebook να τα δημιουργήσει. Σε κάθε notebook το Cell 0 τρέχει:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")
```

Δεν χρειάζεται τίποτα manual.

**Option B — Manual (αν προτιμάτε)**:

1. Click **Catalog** στο αριστερό sidebar
2. Expand `workspace` → click `default` → top-right click **Create** → **Schema**
3. Όνομα: `aade` → Create
4. Στο νέο schema `aade` → click **Create** → **Volume**
5. Όνομα: `aade_data` → Type: **Managed** → Create

Επανάλαβε για volume `mydata_raw` αν θα κάνεις και Lab 3.

---

## Βήμα 3 — Δημιουργία Workspace folder structure

1. Click **Workspace** στο αριστερό sidebar
2. Πλοήγηση: `Users` → `<your-email>`
3. Click **Create** (πάνω δεξιά) → **Folder** → όνομα `data-engineer-training`
4. Μπες μέσα → Create → **Folder** → όνομα `Day2`
5. Επιστροφή πίσω → Create → **Folder** → όνομα `Day3`

**Αποτέλεσμα**:
```
Users / your-email /
└── data-engineer-training /
    ├── Day2 /
    └── Day3 /
```

---

## Βήμα 4 — Import notebooks από GitHub

Για κάθε notebook, ακολούθησε αυτό το pattern (αντικατέστησε `<URL>` με το συγκεκριμένο):

### Method A — Import από URL (FASTEST)

1. Open the GitHub raw URL σε browser tab
2. Right-click → **Save As** → δώσε όνομα (π.χ. `Lab1_LIVEDEMO.py`) στον υπολογιστή
3. Στο Databricks → Workspace → Day2 (ή Day3) → click **Create** → **Notebook** → **Import**
4. Drag-and-drop το `.py` αρχείο
5. Click **Import**

### Method B — Curl + Import (terminal users)

```bash
curl -O https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab1_LIVEDEMO_FreeEdition.py
```

Μετά: Workspace → folder → Right-click → Import → drag-drop.

### URLs για όλα τα notebooks

```
# Day 2
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab1_LIVEDEMO_FreeEdition.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab2_EXERCISE2_Payments.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/Lab3_MYDATA_CLEANUP_Demo.py

# Day 3
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/data_contract_validation_notebook.py
https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day3/Lineage_History_Notebook.py
```

---

## Βήμα 5 — Run notebook (αυτόματη φόρτωση CSVs)

Κάθε notebook έχει ένα **Cell 0** που:

1. Δημιουργεί schema + volume (idempotent)
2. **Κατεβάζει αυτόματα τα CSVs** από το GitHub repo

Απλά:

1. Άνοιξε το imported notebook
2. Click **Run All** (top toolbar)
3. Περίμενε ~30 sec → βλέπεις:
   ```
   ✅ workspace.aade.aade_data ready
   ✅ declarations.csv
   ✅ taxpayers.csv
   ...
   ```

**Δεν χρειάζεται manual upload**.

---

## 🆘 Backup: Manual upload CSVs (αν GitHub δεν είναι accessible)

Σπάνια — αλλά αν είσαι πίσω από corporate firewall που blocks raw.githubusercontent.com:

### Option 1 — Drag-drop στο Volume (Catalog Explorer UI)

1. Κατέβασε όλα τα CSVs τοπικά:
   - https://github.com/sattip/dataengineer-labs/tree/main/Day2 (download manually)
   - https://github.com/sattip/dataengineer-labs/tree/main/Day3
2. Στο Databricks → **Catalog** → `workspace` → `aade` → `Volumes` → `aade_data`
3. Click **Upload to this volume** (top-right button)
4. **Drag-and-drop** όλα τα CSVs ταυτόχρονα
5. Click **Upload**

**Verification**: `dbutils.fs.ls("/Volumes/workspace/aade/aade_data")` πρέπει να δείχνει όλα τα αρχεία.

### Option 2 — Upload via SQL Editor

```sql
-- Δεν λειτουργεί direct upload, αλλά μπορείς να φτιάξεις external location
-- Συστήνεται μόνο σε corporate setups
```

---

## 📂 Files που χρειάζονται ανά Lab

| Lab | CSV Files | Volume |
|---|---|---|
| **Day 2 — Lab 1** | `declarations.csv`, `taxpayers.csv`, `doy.csv`, `employees.csv` | `aade_data` |
| **Day 2 — Lab 2** | `payments.csv` + 4 master | `aade_data` |
| **Day 2 — Lab 3** | `mydata_invoices_MESSY.csv` | `mydata_raw` |
| | + 4 master CSVs | `aade_data` |
| **Day 3 — Άσκηση 3** | `declarations.csv`, `doy.csv`, `employees.csv`, `taxpayers.csv`, `aade_declarations_data_contract.yaml` | `aade_data` |
| **Day 3 — Άσκηση 4** | (κανένα — διαβάζει tables από Άσκηση 3) | `aade_data` |

> **Σημείωση**: Day 2 και Day 3 χρησιμοποιούν τα **ίδια** master CSVs (`declarations`, `taxpayers`, `doy`, `employees`). Αν τρέξεις πρώτα Day 2 Lab 1, δεν χρειάζεται να ξανακατεβάσεις για Day 3 — τα notebooks κάνουν idempotent check.

---

## 🎯 End-to-end test (πριν το live class)

Τρέξε αυτό σε νέο Python notebook για να επιβεβαιώσεις ότι όλα δουλεύουν:

```python
import urllib.request, os

# Setup
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

# Download test file
url = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day2/declarations.csv"
target = "/Volumes/workspace/aade/aade_data/declarations.csv"
urllib.request.urlretrieve(url, target)

# Read it
df = spark.read.csv(target, header=True, inferSchema=True)
print(f"✅ {df.count()} rows loaded")
df.show(3)
```

**Expected output**:
```
✅ 300 rows loaded
+--------+----------+--------+...
|ΔηλωσηID|Ημερομηνία|     ΑΦΜ|
+--------+----------+--------+...
|    9001|2024-01-15|12345678|
|    9002|2024-01-16|23456789|
|    9003|2024-01-17|34567890|
+--------+----------+--------+...
```

Αν αυτό δουλέψει, **όλα τα labs θα δουλέψουν**.

---

## 🐛 Troubleshooting

| Πρόβλημα | Αιτία | Λύση |
|---|---|---|
| `urllib.error.HTTPError: 404` | Λάθος URL | Verify το repo URL: `sattip/dataengineer-labs` |
| `urllib.error.URLError: Connection refused` | Corporate firewall blocks GitHub raw | Χρησιμοποίησε Manual Upload (Option 1 παραπάνω) |
| `Permission denied` στο volume write | Schema/volume δεν υπάρχει | Τρέξε ξανά `CREATE SCHEMA` + `CREATE VOLUME` |
| `[TABLE_OR_VIEW_NOT_FOUND] tax_declarations_silver` σε Άσκηση 4 | Δεν τρέξανε πρώτα Άσκηση 3 | Cell 0.5 του Άσκηση 4 κάνει auto-bootstrap. Run again. |
| Compute idle / cluster not started | Free Edition idle timeout | Click **Connect** στο top-right του notebook → Serverless |
| Greek column names error | Spark SQL needs backticks | Τα notebooks το κάνουν αυτό αυτόματα |

---

## 📞 Support

- **GitHub Issues**: https://github.com/sattip/dataengineer-labs/issues
- **Live class Slack**: #data-eng-training
- **Backup**: Email με attachments (5 CSVs + 5 notebooks) διαθέσιμο από instructor

---

## ✅ Final Checklist (πριν το live class)

- [ ] Databricks Free Edition account ενεργό
- [ ] Workspace folder `data-engineer-training/Day2/` και `Day3/` έτοιμα
- [ ] Schema `workspace.aade` υπάρχει
- [ ] Volume `aade_data` υπάρχει
- [ ] End-to-end test (παραπάνω) τρέχει επιτυχώς
- [ ] Όλα τα 5 notebooks imported στους σωστούς φακέλους
