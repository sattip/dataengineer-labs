# Databricks notebook source
# MAGIC %md
# MAGIC # Άσκηση Ημέρας 4 — ML Model Training (Fraud Detection)
# MAGIC **Ρόλος: Μηχανικοί Δεδομένων**
# MAGIC
# MAGIC ## 🔗 Source URL
# MAGIC > ```
# MAGIC > https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/ML_Model_Notebook.py
# MAGIC > ```
# MAGIC
# MAGIC **Σενάριο ΑΑΔΕ**: εκπαιδεύουμε μοντέλο που εντοπίζει **ύποπτους φορολογούμενους**
# MAGIC (πιθανότητα audit). 50.000 συνθετικές εγγραφές, 3.7% είναι flagged.
# MAGIC
# MAGIC **Συνέχεια από**: `Feature_Engineering_Notebook.py` (αν δεν το έχεις τρέξει, OK —
# MAGIC αυτό κατεβάζει δικό του dataset).
# MAGIC
# MAGIC **Τι θα μάθεις:**
# MAGIC - Train/test **stratified split** (κρίσιμο για imbalanced data)
# MAGIC - Train Random Forest classifier
# MAGIC - Αξιολόγηση: accuracy, AUC, confusion matrix
# MAGIC - **MLflow tracking** (αυτόματο στο Databricks)
# MAGIC - Feature importance ranking
# MAGIC
# MAGIC **Διάρκεια:** ~20-25'
# MAGIC **Περιβάλλον**: Databricks Free Edition (Serverless)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 1: Download Dataset
# MAGIC
# MAGIC Κατεβάζουμε το `taxpayer_features.csv` από το public repo.
# MAGIC 50.000 συνθετικοί φορολογούμενοι ΑΑΔΕ με 11 features.

# COMMAND ----------

import urllib.request
import os

# Unity Catalog Volume setup (αντί για /tmp). Idempotent.
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

volume_dir = "/Volumes/workspace/aade/aade_data"
os.makedirs(volume_dir, exist_ok=True)

url = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/taxpayer_features.csv"
local_path = f"{volume_dir}/taxpayer_features.csv"
urllib.request.urlretrieve(url, local_path)
print(f"✓ Κατέβηκε στο Volume: {local_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 2: Φόρτωση & Quick EDA
# MAGIC
# MAGIC Διαβάζουμε σε Spark DataFrame, μετά μετατρέπουμε σε pandas για τα μετέπειτα
# MAGIC βήματα ML (sklearn χρειάζεται pandas).
# MAGIC
# MAGIC **Target = `is_flagged`** (1 = ύποπτος, 0 = κανονικός).

# COMMAND ----------

import pandas as pd

df = pd.read_csv(local_path)
print(f"Shape: {df.shape}")
print(f"\nColumns: {list(df.columns)}")
print(f"\n=== Target distribution ===")
print(df['is_flagged'].value_counts())
print(f"\nPositive rate: {df['is_flagged'].mean()*100:.2f}%")
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 3: Class Imbalance — Γιατί έχει σημασία
# MAGIC
# MAGIC **3.7% positive rate** σημαίνει imbalanced dataset. Ένα «χαζό» μοντέλο που λέει
# MAGIC «κανείς δεν είναι ύποπτος» θα έχει **96.3% accuracy** — τέλειο score, χωρίς να
# MAGIC κάνει τίποτα χρήσιμο.
# MAGIC
# MAGIC Γι' αυτό:
# MAGIC - Χρησιμοποιούμε **stratified split** για να διατηρηθεί η αναλογία
# MAGIC - Χρησιμοποιούμε **AUC** αντί για accuracy
# MAGIC - Χρησιμοποιούμε **class_weight='balanced'** στο μοντέλο

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 4: Επιλογή Features & Target

# COMMAND ----------

# Numerical features (αφαιρούμε ID columns + categorical)
feature_cols = [
    'income',
    'declarations_last_3y',
    'avg_declaration_amount',
    'days_since_last_audit',
    'sector_risk_score',
    'income_volatility',
    'declared_employees',
]

# One-hot encode το sector
df_encoded = pd.get_dummies(df, columns=['sector'], prefix='sector')
sector_cols = [c for c in df_encoded.columns if c.startswith('sector_')]

X = df_encoded[feature_cols + sector_cols]
y = df_encoded['is_flagged']

print(f"Features: {X.shape[1]}")
print(f"Records:  {X.shape[0]}")
print(f"\nFeature columns:")
for c in X.columns:
    print(f"  - {c}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 5: Train/Test Split (Stratified)
# MAGIC
# MAGIC `stratify=y` διατηρεί την αναλογία 96:4 και στα δύο splits.
# MAGIC Χωρίς αυτό, **μπορεί να καταλήξουμε με 0 ύποπτους στο test set** → ψεύτικα καλά scores.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    stratify=y,           # ⭐ διατήρηση αναλογίας
    random_state=42,
)

print(f"Train: {X_train.shape[0]} ({y_train.mean()*100:.2f}% positive)")
print(f"Test:  {X_test.shape[0]} ({y_test.mean()*100:.2f}% positive)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 6: Train Random Forest
# MAGIC
# MAGIC `class_weight='balanced'` δίνει βάρος στην minority class (flagged).
# MAGIC Χωρίς αυτό, το μοντέλο θα μάθει «πες πάντα 0».
# MAGIC
# MAGIC **MLflow autologging**: στο Databricks το mlflow logging γίνεται αυτόματα.
# MAGIC Όλες οι παράμετροι, metrics, και το model artifact αποθηκεύονται.

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier

# Autologging για όλο το sklearn
mlflow.sklearn.autolog()

with mlflow.start_run(run_name="aade_fraud_rf_v1"):
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        class_weight='balanced',  # ⭐ για imbalanced data
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    print("✓ Model trained")
    print(f"  Trees: {model.n_estimators}")
    print(f"  Max depth: {model.max_depth}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 7: Αξιολόγηση
# MAGIC
# MAGIC 4 metrics για imbalanced classification:
# MAGIC - **AUC**: συνολική ικανότητα διάκρισης (0.5 = τυχαίο, 1.0 = τέλειο)
# MAGIC - **Precision**: από όσους flag-άρουμε, πόσοι είναι όντως ύποπτοι;
# MAGIC - **Recall**: από όλους τους ύποπτους, πόσους πιάνουμε;
# MAGIC - **Confusion matrix**: αναλυτικά counts

# COMMAND ----------

from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score,
    f1_score, confusion_matrix, classification_report,
)

y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

auc = roc_auc_score(y_test, y_proba)
prec = precision_score(y_test, y_pred)
rec = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

print(f"AUC:        {auc:.4f}")
print(f"Precision:  {prec:.4f}  (από όσους flag-άρουμε, πόσοι ύποπτοι)")
print(f"Recall:     {rec:.4f}  (από όλους ύποπτους, πόσους πιάνουμε)")
print(f"F1:         {f1:.4f}")

print(f"\n=== Confusion Matrix ===")
cm = confusion_matrix(y_test, y_pred)
print(f"               Predicted 0   Predicted 1")
print(f"Actual 0:      {cm[0][0]:6d}        {cm[0][1]:6d}")
print(f"Actual 1:      {cm[1][0]:6d}        {cm[1][1]:6d}")

print(f"\n=== Classification Report ===")
print(classification_report(y_test, y_pred, target_names=['Κανονικός', 'Ύποπτος']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 8: Feature Importance
# MAGIC
# MAGIC Ποια features επηρέασαν περισσότερο το μοντέλο;
# MAGIC
# MAGIC *Σημείωση*: αυτή είναι **global importance** — μέσος όρος πάνω σε όλα τα predictions.
# MAGIC Για **per-prediction** εξήγηση χρειαζόμαστε **SHAP** (επόμενο βήμα).

# COMMAND ----------

import numpy as np

importances = pd.DataFrame({
    'feature': X.columns,
    'importance': model.feature_importances_,
}).sort_values('importance', ascending=False)

print("=== Top 10 Features ===")
print(importances.head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 9: Παράδειγμα Predictions με Probability
# MAGIC
# MAGIC Δείχνουμε 5 ύποπτες περιπτώσεις από το test set με το **score** του μοντέλου.
# MAGIC Στην παραγωγή, αυτό είναι το score που θα έβλεπε ο elegctής.

# COMMAND ----------

# Πάρε τις 5 πιο high-risk predictions
test_with_scores = X_test.copy()
test_with_scores['true_label'] = y_test.values
test_with_scores['risk_score'] = y_proba

top_risk = test_with_scores.nlargest(5, 'risk_score')[
    ['income', 'sector_risk_score', 'income_volatility',
     'true_label', 'risk_score']
]
print("=== Top 5 High-Risk Cases ===")
print(top_risk.to_string())

print("\nΕπεξήγηση: όσο πιο κοντά στο 1.0 το risk_score, τόσο πιο ύποπτος.")
print("Στην παραγωγή ο elegctής θα δει αυτό το score + SHAP explanation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Βήμα 10: Σύνοψη & Επόμενα Βήματα
# MAGIC
# MAGIC **Τι κάναμε:**
# MAGIC - Φορτώσαμε 50K φορολογούμενους με 7 numerical + 1 categorical feature
# MAGIC - Stratified split (διατήρηση 96:4 αναλογίας)
# MAGIC - Random Forest με class balancing
# MAGIC - MLflow auto-logging (όλα τα params + metrics + model)
# MAGIC - Feature importance + sample predictions
# MAGIC
# MAGIC **Τι λείπει για production-ready:**
# MAGIC | Βήμα | Εργαλείο |
# MAGIC |---|---|
# MAGIC | Hyperparameter tuning | `GridSearchCV` ή `Optuna` |
# MAGIC | Cross-validation | `StratifiedKFold` |
# MAGIC | Per-prediction explainability | **SHAP** values |
# MAGIC | Bias audit | Fairness metrics ανά region/sector |
# MAGIC | Model registry | `mlflow.register_model()` |
# MAGIC | Online endpoint | Azure ML / Databricks Serving |
# MAGIC | Drift monitoring | PSI, KS test |
# MAGIC | Automated retraining | Scheduled job + drift trigger |
# MAGIC
# MAGIC **EU AI Act note**: αυτό είναι **high-risk system**. Πριν deployment απαιτείται:
# MAGIC bias audit, explainability για κάθε prediction (SHAP), human-in-the-loop,
# MAGIC documentation (model card), audit logs.
# MAGIC
# MAGIC **Δες το MLflow run** στο **Experiments tab** του workspace — όλες οι παράμετροι
# MAGIC και metrics έχουν αυτο-loggαριστεί.
