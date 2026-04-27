# Άσκηση Ημέρας 4 — Git Workflow & CI/CD

**Σκοπός**: στην πράξη πώς ένα DE notebook μπαίνει σε **version control** + αποκτά **αυτόματα tests** + προωθείται μέσω **Pull Request**.

**Διάρκεια**: 30' (5' setup, 15' coding, 5' PR & CI, 5' debrief)

---

## 📁 Δομή του παραδείγματος

```
Git_CICD_Example/
├── notebooks/
│   ├── __init__.py
│   └── clean_citizens.py        ← ο κώδικας που τεστάρουμε
├── tests/
│   ├── __init__.py
│   └── test_clean_citizens.py   ← unit tests
├── .github/workflows/
│   └── ci.yml                   ← CI pipeline (GitHub Actions)
└── README.md                    ← αυτό το αρχείο
```

## 🎯 Τι μαθαίνετε

1. **Git basics**: clone, branch, commit, push, PR, merge
2. **Unit tests** για PySpark functions
3. **CI pipeline** με GitHub Actions που τρέχει tests αυτόματα
4. **Pull Request workflow**: code review πριν το merge

---

## 🚀 Walkthrough — Βήμα-βήμα

### Prerequisites

- GitHub account (free)
- Local Git installed (`git --version`)
- Python 3.10+ με `pyspark` & `pytest`
- VS Code ή PyCharm

---

### Βήμα 1 — Repository setup (5')

Στο GitHub:
- New repository → όνομα `de-day4-cicd-lab` → Private → Add README → Create

Στο τερματικό:
```bash
git clone https://github.com/<your-user>/de-day4-cicd-lab.git
cd de-day4-cicd-lab
mkdir -p notebooks tests .github/workflows
```

---

### Βήμα 2 — Feature branch + κώδικας (10')

```bash
git checkout -b feat/clean-citizens
```

Δημιουργήστε `notebooks/clean_citizens.py` (αντιγράψτε από το παράδειγμα).

---

### Βήμα 3 — Unit test (5')

Δημιουργήστε `tests/test_clean_citizens.py` (αντιγράψτε από το παράδειγμα).

**Δοκιμάστε τοπικά**:
```bash
pip install pyspark==3.5.0 pytest
pytest -v
```

✅ Πρέπει να δείτε `2 passed`.

---

### Βήμα 4 — CI workflow (5')

Δημιουργήστε `.github/workflows/ci.yml` (αντιγράψτε από το παράδειγμα).

---

### Βήμα 5 — Pull Request (5')

```bash
git add .
git commit -m 'feat: add citizen cleanup with tests'
git push origin feat/clean-citizens
```

Στο GitHub UI:
1. **Compare & pull request**
2. Δείτε το πράσινο ✅ (CI passed) στο PR
3. **Squash & merge**

---

## ⚠️ Common pitfalls

| Πρόβλημα | Λύση |
|---|---|
| `Spark δεν τρέχει στον runner` | `pip install pyspark` φέρνει standalone — δεν χρειάζεται cluster |
| `ImportError: No module named notebooks` | Σιγουρευτείτε ότι υπάρχει `__init__.py` στους φακέλους + τρέξτε pytest από project root |
| `CI fails: pytest not found` | Στο `ci.yml`: `pip install pytest` πριν το `pytest -v` |

---

## 🤔 Debrief — Συζήτηση

1. **Πόσο χρόνο πήρε το test setup;**
   *Συνήθως 5-10' πρώτη φορά, μετά 1-2'*

2. **Τι θα κάνατε αν το CI απέτυχε σε PR;**
   Σχόλιο στο PR thread → fix locally → push ξανά → CI ξανατρέχει αυτόματα

3. **Πώς θα μεταφέρατε αυτή τη ροή στο Databricks/Fabric;**
   - **Databricks**: Repos integration + Jobs API για test execution
   - **Fabric**: Deployment Pipelines για promotion μεταξύ Dev/Test/Prod

---

## 🔗 Επόμενα βήματα

Αυτή η νοοτροπία επεκτείνεται:
- **Στο Day 4 (Feature Engineering Notebook)**: ίδιο pattern — features σε version control + tests
- **Στο Day 5+**: ολόκληρα ML pipelines με CI/CD που τρέχουν training + validation + deployment
