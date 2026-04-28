# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Πρακτική Άσκηση — MLOps End-to-End: Deploy & Monitor
# MAGIC
# MAGIC **Ρόλος:** Μηχανικός Δεδομένων / MLOps Engineer στην ΑΑΔΕ
# MAGIC **Διάρκεια:** ~35'
# MAGIC **Περιβάλλον:** Databricks Free Edition (Serverless) με Unity Catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Στόχος της άσκησης
# MAGIC
# MAGIC > **Να πάρετε ένα ήδη εκπαιδευμένο ML μοντέλο και να το φτάσετε σε production-ready
# MAGIC > κατάσταση με registry, deployment endpoint, batch scoring και drift monitoring —
# MAGIC > όλα τα κομμάτια που χρειάζονται για να είναι το μοντέλο νόμιμο, ασφαλές και ζωντανό
# MAGIC > σε δημόσιο φορέα.**
# MAGIC
# MAGIC ## 🧭 Σενάριο
# MAGIC
# MAGIC Έχετε ένα εκπαιδευμένο μοντέλο (sklearn/xgboost) για prediction κινδύνου κατάθεσης ΑΑΔΕ.
# MAGIC
# MAGIC Στόχος:
# MAGIC → Καταχωρήστε το στο MLflow Registry
# MAGIC → Deploy σε Databricks Model Serving endpoint
# MAGIC → Γράψτε batch scoring job
# MAGIC → Στήστε drift monitoring
# MAGIC
# MAGIC ## ❓ Γιατί έχει σημασία
# MAGIC
# MAGIC Ένα μοντέλο που μένει σε notebook δεν λύνει κανένα πρόβλημα.
# MAGIC Πρέπει να ζει σε production. Με governance. Με monitoring. Με rollback.
# MAGIC Αυτό είναι το lab.
# MAGIC
# MAGIC ## 📋 Τα 5 βήματα (από τη διαφάνεια)
# MAGIC
# MAGIC | # | Βήμα | Τι κάνουμε |
# MAGIC |---|---|---|
# MAGIC | 1 | **Register** | Καταχωρήστε το μοντέλο στο MLflow Registry με tag 'staging' και ονομασία 'aade-risk-scorer' |
# MAGIC | 2 | **Promote** | Promote σε 'production' μέσω registry UI ή API (`mlflow.transition_model_version_stage`) |
# MAGIC | 3 | **Batch Scoring** | Δημιουργήστε batch scoring notebook που κάνει `spark_predict` πάνω σε feature_store table |
# MAGIC | 4 | **Real-time Endpoint** | Στήστε Databricks Model Serving endpoint (REST API) και κάντε test με curl/Postman |
# MAGIC | 5 | **Drift Monitoring** | Καταγράψτε baseline distribution για 3 features και γράψτε drift detector με PSI ή KS test που τρέχει daily |
# MAGIC
# MAGIC > **💡 Tip από το slide:** Για production deployment στον δημόσιο τομέα, προσθέστε πάντα:
# MAGIC > (1) audit log κάθε prediction με timestamp & input hash
# MAGIC > (2) human-in-the-loop review για high-risk predictions
# MAGIC > (3) explainability με SHAP για κάθε flagged decision (απαίτηση EU AI Act)
# MAGIC
# MAGIC ## 📦 Παραδοτέα
# MAGIC
# MAGIC - **Registered model**: `aade-risk-scorer` με versions
# MAGIC - **Promoted version**: σε stage Production
# MAGIC - **Batch predictions table**: `workspace.aade.risk_predictions`
# MAGIC - **Drift report table**: `workspace.aade.drift_reports`
# MAGIC - **Audit log table**: `workspace.aade.prediction_audit`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 0: Setup — Schema, Volume, και training ενός baseline μοντέλου
# MAGIC
# MAGIC Για να δουλέψουμε με ένα πραγματικό μοντέλο, εκπαιδεύουμε γρήγορα ένα baseline
# MAGIC Random Forest σε συνθετικά δεδομένα ΑΑΔΕ. Σε production θα είχατε ήδη ένα
# MAGIC trained model από προηγούμενο pipeline.

# COMMAND ----------

import os
import urllib.request

# CRITICAL: Set MLFLOW_REGISTRY_URI BEFORE any mlflow import/call.
# Σε Spark Connect (Free Edition / Serverless), το MlflowClient() καλεί
# spark.conf.get('spark.mlflow.modelRegistryUri') και σπάει με
# CONFIG_NOT_AVAILABLE. Με το env var, το mlflow short-circuits και
# δεν φτάνει ποτέ στο spark.conf call.
os.environ["MLFLOW_REGISTRY_URI"] = "databricks-uc"

# Unity Catalog setup (idempotent)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.aade")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.aade.aade_data")

volume_dir = "/Volumes/workspace/aade/aade_data"
os.makedirs(volume_dir, exist_ok=True)

# Κατεβάζουμε το ίδιο dataset που χρησιμοποίησε το ML_Model_Notebook
url = "https://raw.githubusercontent.com/sattip/dataengineer-labs/main/Day4/taxpayer_features.csv"
local_path = f"{volume_dir}/taxpayer_features.csv"
if not os.path.exists(local_path):
    urllib.request.urlretrieve(url, local_path)
print(f"✓ Dataset: {local_path}")

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import mlflow
import mlflow.sklearn

# Δεν καλούμε mlflow.set_experiment() ρητά — η Databricks ορίζει αυτόματα
# το notebook path ως experiment. Σε Free Edition / Serverless το explicit
# set_experiment() καλεί spark.conf για το registry URI και σπάει.

df = pd.read_csv(local_path)

features = ["income", "expenses", "tax_paid", "declaration_count", "age", "region_code"]
target = "is_flagged"

# Filter out columns that don't exist (handle dataset variations)
features = [f for f in features if f in df.columns]
print(f"Using features: {features}")

X = df[features].fillna(df[features].median(numeric_only=True))
y = df[target]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Train baseline model
with mlflow.start_run(run_name="aade_risk_baseline") as run:
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_train, y_train)

    auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
    mlflow.log_metric("auc", auc)
    mlflow.log_params({"n_estimators": 100, "max_depth": 10})

    # Log model
    mlflow.sklearn.log_model(model, artifact_path="model")
    run_id = run.info.run_id

print(f"\n✓ Baseline trained. AUC = {auc:.4f}")
print(f"✓ Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Register στο MLflow Registry με tag 'staging'
# MAGIC
# MAGIC ### 1α. Η ιδέα
# MAGIC
# MAGIC Το trained model ζει αυτή τη στιγμή σαν artifact ενός run. Δεν μπορεί να βρεθεί
# MAGIC εύκολα από άλλη ομάδα. Δεν έχει version. Δεν έχει approval workflow.
# MAGIC
# MAGIC Με `mlflow.register_model()` γίνεται **catalog-level entity** με versioning,
# MAGIC stages, lineage και metadata.
# MAGIC
# MAGIC ### 1β. Το όνομα
# MAGIC
# MAGIC Στο Unity Catalog ο πλήρης δρόμος είναι `catalog.schema.model_name`.
# MAGIC Στην περίπτωσή μας: `workspace.aade.aade-risk-scorer`.

# COMMAND ----------

from mlflow.tracking import MlflowClient

# Επιλέγουμε registry mode ανάλογα με το workspace.
# Auto-detect: αν τρέχουμε σε Spark Connect (Free Edition / Serverless) και
# το spark.mlflow.modelRegistryUri δεν είναι διαθέσιμο, dropάρουμε σε
# in-run mode και δείχνουμε τη ροή αλλιώς.
# Επιχειρούμε UC registration. Αν αποτύχει σε Free Edition/Serverless workspace,
# fallback σε in-run mode όπου το μοντέλο φορτώνεται από runs:/run_id/model.
USE_REGISTRY = True
USE_UC = True  # Default για UC-enabled workspaces (αν αποτύχει, dropάρει σε False παρακάτω)
model_name = "workspace.aade.aade_risk_scorer"

client = MlflowClient()

# Register (ή skip αν registry not available)
mv = None
model_uri = f"runs:/{run_id}/model"
try:
    mv = mlflow.register_model(model_uri=model_uri, name=model_name)
    print(f"✓ Registered στο UC: {model_name} version {mv.version}")
except Exception as e:
    err_str = str(e)
    print(f"⚠️  UC registration απέτυχε: {type(e).__name__}: {err_str[:200]}")
    # Δοκιμάζουμε legacy workspace registry
    try:
        os.environ["MLFLOW_REGISTRY_URI"] = "databricks"
        mlflow.set_registry_uri("databricks")
        client = MlflowClient()
        model_name = "aade_risk_scorer"
        USE_UC = False
        mv = mlflow.register_model(model_uri=model_uri, name=model_name)
        print(f"✓ Registered στο workspace registry: {model_name} version {mv.version}")
    except Exception as e2:
        USE_REGISTRY = False
        print(f"⚠️  Registry εντελώς disabled: {type(e2).__name__}")
        print("   Συνεχίζουμε σε in-run mode — model URI: runs:/run_id/model")

# Demo placeholder version όταν registry δεν δουλεύει
if mv is None:
    class _FakeMv:
        version = "1"
    mv = _FakeMv()

# Set staging tag μόνο αν έχουμε registry
if USE_REGISTRY:
    try:
        client.set_model_version_tag(
            name=model_name,
            version=mv.version,
            key="stage",
            value="staging",
        )
        print(f"✓ Tag set: stage=staging")
    except Exception as e:
        print(f"⚠️  Tag setting skipped: {type(e).__name__}")

print(f"✓ Registered: {model_name}")
print(f"✓ Version: {mv.version}")
print(f"✓ Tag set: stage=staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1γ. Επιβεβαίωση
# MAGIC
# MAGIC Δείτε το registered model + version + tags.

# COMMAND ----------

if USE_REGISTRY:
    try:
        print("=== Registered model versions ===")
        for v in client.search_model_versions(f"name='{model_name}'"):
            print(f"  → version {v.version} | run_id: {v.run_id} | tags: {v.tags}")
    except Exception as e:
        print(f"⚠️  search_model_versions skipped: {type(e).__name__}: {str(e)[:200]}")
else:
    print("⚠️  Registry not available — skipping search_model_versions demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 2: Promote σε 'production'
# MAGIC
# MAGIC ### 2α. Η μετάβαση
# MAGIC
# MAGIC Στο Unity Catalog δεν χρησιμοποιούμε πλέον τα παλιά stages (None/Staging/Production/Archived).
# MAGIC Αντί γι' αυτό χρησιμοποιούμε **aliases** — αναφορές με όνομα που δείχνουν σε συγκεκριμένη version.
# MAGIC
# MAGIC Έτσι, ο `production` alias μπορεί να αλλάζει version χωρίς να αλλάζει ο κώδικας
# MAGIC που τον χρησιμοποιεί.
# MAGIC
# MAGIC ### 2β. Πρακτικά
# MAGIC
# MAGIC Δίνουμε στον alias `production` την τρέχουσα version. Παράλληλα ενημερώνουμε το tag.
# MAGIC
# MAGIC > **Σημείωση:** Σε production ΑΑΔΕ, αυτό το βήμα **δεν** το κάνει αυτόματα ένα CI job.
# MAGIC > Απαιτείται 2 approvals: data scientist + compliance officer.

# COMMAND ----------

# Promote to production
if not USE_REGISTRY:
    print("⚠️  Promotion step skipped — registry not available σε αυτό το workspace.")
    print("   Στη production θα τρέχατε είτε set_registered_model_alias (UC) είτε transition_model_version_stage (workspace).")
else:
    try:
        if USE_UC:
            client.set_registered_model_alias(
                name=model_name,
                alias="production",
                version=mv.version,
            )
            print(f"✓ Alias 'production' → version {mv.version}")
        else:
            client.transition_model_version_stage(
                name=model_name,
                version=mv.version,
                stage="Production",
                archive_existing_versions=True,
            )
            print(f"✓ Stage transitioned to Production για version {mv.version}")

        client.set_model_version_tag(
            name=model_name,
            version=mv.version,
            key="stage",
            value="production",
        )
        print(f"✓ Tag updated: stage=production")
    except Exception as e:
        print(f"⚠️  Promotion error: {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **🔍 Τι γίνεται στα παλαιότερα Databricks workspaces:**
# MAGIC >
# MAGIC > Στα non-UC workspaces θα χρησιμοποιούσατε:
# MAGIC > ```python
# MAGIC > client.transition_model_version_stage(
# MAGIC >     name=model_name,
# MAGIC >     version=mv.version,
# MAGIC >     stage="Production",
# MAGIC >     archive_existing_versions=True,
# MAGIC > )
# MAGIC > ```
# MAGIC > Στο σλάιντ αυτή είναι η εντολή που αναφέρεται. Στο UC χρησιμοποιούμε aliases — κάνουν την ίδια δουλειά πιο ευέλικτα.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 3: Batch Scoring με spark_udf σε feature store table
# MAGIC
# MAGIC ### 3α. Η ιδέα
# MAGIC
# MAGIC Το production μοντέλο πρέπει να σκοράρει εκατομμύρια εγγραφές καθημερινά.
# MAGIC Το πιο γρήγορο pattern: φορτώνουμε το μοντέλο ως **Spark UDF** και το εφαρμόζουμε
# MAGIC σε ολόκληρο DataFrame παράλληλα.
# MAGIC
# MAGIC ### 3β. Πώς δουλεύει
# MAGIC
# MAGIC Το `mlflow.pyfunc.spark_udf` παίρνει το URI του μοντέλου και επιστρέφει μια
# MAGIC συνάρτηση Spark που μπορούμε να εφαρμόσουμε με `withColumn`.

# COMMAND ----------

from pyspark.sql.functions import struct, current_timestamp, sha2, concat_ws, col, lit
from pyspark.sql.types import DoubleType

# Φορτώνουμε το μοντέλο ως Spark UDF (για παραλληλισμό)
# Σε Free Edition χρησιμοποιούμε runs:/ URI απευθείας (registry δεν δουλεύει)
if not USE_REGISTRY:
    prod_model_uri = f"runs:/{run_id}/model"
elif USE_UC:
    prod_model_uri = f"models:/{model_name}@production"
else:
    prod_model_uri = f"models:/{model_name}/Production"

try:
    predict_udf = mlflow.pyfunc.spark_udf(
        spark,
        model_uri=prod_model_uri,
        result_type=DoubleType(),
    )
    print(f"✓ Model loaded as spark_udf από: {prod_model_uri}")
except Exception as e:
    # Fallback σε run-uri αν το registry-based load αποτύχει
    print(f"⚠️  Load απέτυχε, fallback σε runs URI: {type(e).__name__}")
    prod_model_uri = f"runs:/{run_id}/model"
    predict_udf = mlflow.pyfunc.spark_udf(
        spark,
        model_uri=prod_model_uri,
        result_type=DoubleType(),
    )

# Φορτώνουμε το feature store table
features_sdf = spark.read.csv(local_path, header=True, inferSchema=True)

# Apply prediction
scored = features_sdf.withColumn(
    "risk_score",
    predict_udf(struct(*[col(f) for f in features])),
).withColumn(
    "scored_at", current_timestamp(),
).withColumn(
    "model_version", lit(str(mv.version)),
)

# Audit log: hash των inputs (για EU AI Act compliance)
audit = scored.withColumn(
    "input_hash",
    sha2(concat_ws("|", *[col(f).cast("string") for f in features]), 256),
).select(
    "afm" if "afm" in scored.columns else features[0],
    "scored_at",
    "model_version",
    "risk_score",
    "input_hash",
)

# Save predictions
scored.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.risk_predictions")
audit.write.format("delta").mode("overwrite").saveAsTable("workspace.aade.prediction_audit")

print(f"✓ Predictions saved → workspace.aade.risk_predictions")
print(f"✓ Audit log saved   → workspace.aade.prediction_audit")
print(f"  Total rows scored: {scored.count():,}")

# COMMAND ----------

print("=== Sample predictions ===")
spark.table("workspace.aade.risk_predictions").select(
    *features[:3], "risk_score", "scored_at", "model_version"
).show(5, truncate=False)

print("=== Audit log sample ===")
spark.table("workspace.aade.prediction_audit").show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 4: Real-time Endpoint με Databricks Model Serving
# MAGIC
# MAGIC ### 4α. Η διαφορά από batch
# MAGIC
# MAGIC Το batch scoring είναι ιδανικό για 5 εκατομμύρια ΑΦΜ τη νύχτα.
# MAGIC Αλλά δεν εξυπηρετεί real-time scenarios — π.χ. όταν το TAXIS θέλει να σκοράρει
# MAGIC μια δήλωση τη στιγμή της υποβολής.
# MAGIC
# MAGIC Για αυτό στήνουμε **Model Serving Endpoint** — ένα REST API που τρέχει 24/7.
# MAGIC
# MAGIC ### 4β. Στο Free Edition
# MAGIC
# MAGIC Το full Databricks Model Serving (autoscaling, GPU, A/B routing) απαιτεί premium SKU.
# MAGIC Στη Free Edition έχουμε περιορισμένη πρόσβαση. Παρακάτω βλέπετε:
# MAGIC → Τη ροή που θα τρέχατε σε production
# MAGIC → Ένα local-equivalent test με `mlflow.pyfunc` για να δείτε πώς θα φαινόταν το response

# COMMAND ----------

# Local-equivalent simulation: τι θα έβλεπε το REST API
import json

try:
    loaded_model = mlflow.pyfunc.load_model(prod_model_uri)
except Exception:
    # Fallback σε run-uri
    loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model")

# Sample payload (JSON όπως θα το έστελνε το TAXIS)
sample_payload = {
    "dataframe_records": [
        df[features].iloc[0].to_dict()
    ]
}
print("=== Sample REST request payload ===")
print(json.dumps(sample_payload, indent=2, default=str))

# Predict
result = loaded_model.predict(pd.DataFrame(sample_payload["dataframe_records"]))
print(f"\n=== Sample REST response ===")
print(json.dumps({"predictions": result.tolist()}, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4γ. Πραγματικό deployment (production reference)
# MAGIC
# MAGIC Σε production workspace με Premium SKU, θα τρέχατε:
# MAGIC
# MAGIC ```bash
# MAGIC databricks serving-endpoints create \
# MAGIC   --name aade-risk-scorer-prod \
# MAGIC   --config '{
# MAGIC     "served_models": [{
# MAGIC       "model_name": "workspace.aade.aade_risk_scorer",
# MAGIC       "model_version": "1",
# MAGIC       "workload_size": "Small",
# MAGIC       "scale_to_zero_enabled": true
# MAGIC     }]
# MAGIC   }'
# MAGIC ```
# MAGIC
# MAGIC Και test:
# MAGIC ```bash
# MAGIC curl -X POST https://<workspace>.azuredatabricks.net/serving-endpoints/aade-risk-scorer-prod/invocations \
# MAGIC   -H "Authorization: Bearer $TOKEN" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{"dataframe_records": [{"income": 45000, "expenses": 18000, ...}]}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 5: Drift Monitoring με PSI και KS test
# MAGIC
# MAGIC ### 5α. Η ιδέα
# MAGIC
# MAGIC Το μοντέλο που deployάραμε σήμερα μπορεί να γίνει άχρηστο σε 6 μήνες — επειδή
# MAGIC ο κόσμος αλλάζει (πληθωρισμός, νέα φορολογικά καθεστώτα, νέοι κλάδοι επιχειρήσεων).
# MAGIC
# MAGIC Πρέπει να παρακολουθούμε:
# MAGIC → **Data drift** — αλλαγή στις κατανομές των features
# MAGIC → **Performance drift** — πτώση AUC/precision όταν έχουμε labels
# MAGIC
# MAGIC ### 5β. Δύο εργαλεία
# MAGIC
# MAGIC | Tool | Τι μετράει | Threshold |
# MAGIC |---|---|---|
# MAGIC | **PSI** (Population Stability Index) | Συνολική απόκλιση κατανομής | <0.1 ok, 0.1-0.2 watch, >0.2 retrain |
# MAGIC | **KS test** (Kolmogorov-Smirnov) | Στατιστική σημαντικότητα διαφοράς | p<0.05 = drift |

# COMMAND ----------

import numpy as np
from scipy import stats

def psi(reference, current, bins=10):
    """Population Stability Index."""
    breakpoints = np.linspace(
        min(reference.min(), current.min()),
        max(reference.max(), current.max()),
        bins + 1,
    )
    ref_hist, _ = np.histogram(reference, bins=breakpoints)
    cur_hist, _ = np.histogram(current, bins=breakpoints)
    ref_pct = ref_hist / max(ref_hist.sum(), 1)
    cur_pct = cur_hist / max(cur_hist.sum(), 1)
    # Avoid log(0) με μικρή σταθερά
    ref_pct = np.where(ref_pct == 0, 1e-6, ref_pct)
    cur_pct = np.where(cur_pct == 0, 1e-6, cur_pct)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def ks(reference, current):
    """Kolmogorov-Smirnov 2-sample test."""
    statistic, pvalue = stats.ks_2samp(reference, current)
    return float(statistic), float(pvalue)


# Καταγράφουμε το baseline (training distribution)
baseline = X_train.copy()

# Προσομοίωση: το current dataset έχει drift σε ένα feature
# (πληθωρισμός 25% στο income — ρεαλιστικό σενάριο για ΑΑΔΕ μετά από 2 χρόνια)
current = X_test.copy()
current["income"] = current["income"] * 1.25  # simulate inflation drift

# Έλεγχος drift για 3 features
drift_results = []
for feat in features[:3]:
    psi_val = psi(baseline[feat].values, current[feat].values)
    ks_stat, ks_pval = ks(baseline[feat].values, current[feat].values)
    drift_results.append({
        "feature": feat,
        "psi": round(psi_val, 4),
        "ks_statistic": round(ks_stat, 4),
        "ks_pvalue": round(ks_pval, 4),
        "drift_detected": psi_val > 0.2 or ks_pval < 0.05,
    })

drift_df = spark.createDataFrame(drift_results)
print("=== Drift Detection Report ===")
drift_df.show(truncate=False)

# Σώζουμε για ιστορικότητα
drift_df = drift_df.withColumn("checked_at", current_timestamp()).withColumn(
    "model_version", lit(str(mv.version))
)
drift_df.write.format("delta").mode("append").saveAsTable("workspace.aade.drift_reports")
print("✓ Drift report saved → workspace.aade.drift_reports")

# COMMAND ----------

# MAGIC %md
# MAGIC **🔍 Τι μάθαμε:**
# MAGIC
# MAGIC - Παρατηρήστε ότι το `income` feature έχει **ψηλό PSI** (>0.2) και
# MAGIC   **πολύ μικρό KS p-value** (<0.05) επειδή το προσομοιώσαμε με 25% inflation
# MAGIC - Αυτό σηματοδοτεί ότι **πρέπει να γίνει retraining**
# MAGIC - Σε production, αυτό το job τρέχει daily και:
# MAGIC   → στέλνει alert στο Teams της ομάδας MLOps
# MAGIC   → εγγράφεται στον drift_reports πίνακα
# MAGIC   → όταν περάσει το threshold, ξεκινά αυτόματα retraining job

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 6: Production-grade additions (από το slide tip)
# MAGIC
# MAGIC Στη διαφάνεια αναφέρονται 3 πράγματα που πρέπει **πάντα** να προσθέτετε σε
# MAGIC production deployment του δημοσίου τομέα. Τα τρία αυτά είναι νομικές υποχρεώσεις
# MAGIC του EU AI Act για high-risk συστήματα.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6α. Audit log κάθε prediction (✓ already done in Step 3)
# MAGIC
# MAGIC Δείτε τον πίνακα `workspace.aade.prediction_audit`. Κάθε prediction έχει:
# MAGIC → timestamp
# MAGIC → input_hash (SHA-256 για non-repudiation)
# MAGIC → model_version
# MAGIC
# MAGIC Σε production, retention 7 χρόνια.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6β. Human-in-the-loop για high-risk predictions
# MAGIC
# MAGIC Για κάθε prediction με score > threshold, **απαγορεύεται** auto-action.
# MAGIC Πρέπει να γραφτεί σε review queue και να εγκριθεί από άνθρωπο.

# COMMAND ----------

# Παράδειγμα: high-risk predictions πάνε σε review queue
high_risk_threshold = 0.8

review_queue = (
    spark.table("workspace.aade.risk_predictions")
    .filter(col("risk_score") > high_risk_threshold)
    .select(*features[:3], "risk_score", "scored_at", "model_version")
)

review_queue.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.aade.high_risk_review_queue"
)

print(f"✓ {review_queue.count():,} predictions στο high-risk review queue")
print("  → Πάνε σε ελεγκτή για manual review πριν γίνει οποιαδήποτε ενέργεια")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6γ. SHAP explainability για κάθε flagged decision
# MAGIC
# MAGIC Όταν ο πολίτης ρωτήσει «γιατί ελέγχθηκα;», πρέπει να μπορούμε να εξηγήσουμε.
# MAGIC Με SHAP, για κάθε prediction παίρνουμε τα top 3 features που οδήγησαν στο score.

# COMMAND ----------

# Εγκατάσταση SHAP αν δεν υπάρχει ήδη
try:
    import shap
except ImportError:
    print("Εγκατάσταση SHAP (μπορεί να χρειαστούν λίγα δευτερόλεπτα)...")
    import subprocess
    subprocess.check_call(["pip", "install", "-q", "shap"])
    import shap

# SHAP για ένα flagged sample
explainer = shap.TreeExplainer(model)
sample = X_test.head(1)
shap_values = explainer.shap_values(sample)

# Για binary classifier, παίρνουμε τα values της θετικής κλάσης
if isinstance(shap_values, list):
    shap_vals = shap_values[1][0]
else:
    shap_vals = shap_values[0]

# Top 3 contributing features
contributions = sorted(
    zip(features, shap_vals),
    key=lambda x: abs(x[1]),
    reverse=True,
)[:3]

print("=== SHAP explanation για 1 flagged prediction ===\n")
print(f"Sample: {sample.iloc[0].to_dict()}")
print(f"\nTop 3 contributing features:")
for feat, contribution in contributions:
    direction = "↑" if contribution > 0 else "↓"
    print(f"  {direction} {feat:25s}: {contribution:+.4f}")

print("\nΑυτή η εξήγηση πάει στο audit table για EU AI Act compliance.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Ολοκλήρωση
# MAGIC
# MAGIC ### 🎓 Τι μάθατε σε αυτή την άσκηση
# MAGIC
# MAGIC 1. **Register** μοντέλο στο Unity Catalog MLflow Registry με tags
# MAGIC 2. **Promote** σε production μέσω alias (modern UC) ή stage (legacy)
# MAGIC 3. **Batch scoring** με `mlflow.pyfunc.spark_udf` για παραλληλισμό
# MAGIC 4. **Real-time endpoint** ροή (production reference + local simulation)
# MAGIC 5. **Drift monitoring** με PSI και KS test
# MAGIC 6. **Production-grade additions** για EU AI Act compliance:
# MAGIC    → Audit log με timestamp + input hash
# MAGIC    → Human-in-the-loop review queue για high-risk
# MAGIC    → SHAP explanations για κάθε flagged decision
# MAGIC
# MAGIC ### 🎯 Common pitfalls (από το slide)
# MAGIC
# MAGIC | Pitfall | Λύση |
# MAGIC |---|---|
# MAGIC | Ξεχνάτε registry tag και χάνετε το model | Πάντα tag στην εγγραφή |
# MAGIC | Endpoint test χωρίς auth → 401 | Σωστά tokens στο header |
# MAGIC | Drift detector χωρίς σταθερό baseline | Πάντα persisted baseline |
# MAGIC | PSI threshold πολύ χαμηλό → false alerts | Tier system: 0.1 / 0.2 |
# MAGIC
# MAGIC ### 💡 Take-home message
# MAGIC
# MAGIC > **Ένα μοντέλο σε notebook δεν λύνει κανένα πρόβλημα.
# MAGIC > Ένα μοντέλο σε production με registry, monitoring, audit trail και explainability
# MAGIC > λύνει πραγματικά προβλήματα — και είναι νόμιμο.**
# MAGIC >
# MAGIC > Αυτή η ροή είναι **το minimum** για να βγει ML σύστημα ΑΑΔΕ σε production μετά το 2027,
# MAGIC > σύμφωνα με την EU AI Act.
