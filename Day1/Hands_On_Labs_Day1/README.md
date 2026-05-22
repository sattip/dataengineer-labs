# Day 1 — Hands-On Labs (Standalone Teaching Notebooks)

Δύο πλήρη Databricks notebooks που έχουν **όλα μέσα**:
- 📚 Θεωρία (15-20')
- 🎯 Εκφώνηση & απαιτήσεις
- ✍️ Empty code cells για coding from scratch
- ✅ Auto-verification

| Lab | Type | Διάρκεια | Παρουσίαση |
|---|---|---|---|
| **Lab1_UC_Foundation.py** | Solo | 80' | Όχι |
| **Lab2_Bronze_Ingestion.py** | Groups 2-3 | 110' | Ναι (2 ομάδες × 10') |

## Πώς το χρησιμοποιείς ως trainer

1. Πες στους μαθητές: «Import στο Databricks workspace» (Workspace → Import → Upload).
2. Δίνεις τη πρώτη ώρα να διαβάσουν τη θεωρία (μαζί ή solo).
3. Μετά «Click Step 1 και γράψτε κώδικα». Trainer περπατά.
4. Auto-verification cell στο τέλος → instant feedback ποιοι πέτυχαν.
5. Lab 2 → 2 ομάδες παρουσιάζουν στο τέλος (live από notebook).

## Auto-bootstrap

Κάθε notebook έχει `Step 0` που δημιουργεί catalog/schema/volume + κατεβάζει τα 5 CSVs από GitHub raw URL. **Idempotent** — μπορεί να ξανατρέξει.

**Prerequisite (τrainer):** Push τα CSVs στο `github.com/sattip/dataengineer/main/Day6/data_for_students/` (δες `Ασκήσεις_GT_Final_Package/DATA_SETUP.docx`).
