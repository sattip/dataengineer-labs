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
# MAGIC
# MAGIC ### 📚 Βασικές έννοιες — γρήγορο glossary
# MAGIC
# MAGIC | Όρος | Τι είναι |
# MAGIC |---|---|
# MAGIC | **Model** | Συνάρτηση που μαθαίνει patterns από data και κάνει προβλέψεις (input → output) |
# MAGIC | **Training** | Η διαδικασία όπου το μοντέλο «μαθαίνει» από ιστορικά δεδομένα |
# MAGIC | **Features** | Οι **input** στήλες (income, expenses, …) που χρησιμοποιεί το μοντέλο |
# MAGIC | **Target** | Η **output** στήλη που θέλουμε να προβλέψουμε (π.χ. is_flagged) |
# MAGIC | **Random Forest** | Ensemble από πολλά decision trees που «ψηφίζουν» — robust, εύκολο να εκπαιδευτεί |
# MAGIC | **AUC** | Area Under ROC Curve — μετρική 0-1, **0.5=τύχη**, **1.0=τέλειο** |
# MAGIC | **train/test split** | Χωρίζουμε τα data: 80% για μάθηση, 20% για αξιολόγηση σε «αόρατα» δεδομένα |
# MAGIC | **stratify** | Διατηρεί την αναλογία κλάσεων (π.χ. 70% legit / 30% flagged) σε train & test |
# MAGIC | **class_weight=balanced** | Δίνει μεγαλύτερο βάρος στη μειοψηφική κλάση ώστε το μοντέλο να μην την αγνοήσει |
# MAGIC | **MLflow** | Open-source εργαλείο για tracking, registry, και deployment μοντέλων |
# MAGIC | **MLflow run** | Μία εκτέλεση training — αποθηκεύει params, metrics, artifacts (model files) |
# MAGIC | **artifact** | Αρχείο σχετικό με το run (model.pkl, plots, εκθέσεις, …) |
# MAGIC
# MAGIC ### 📚 Τι σημαίνει «AUC = 0.85»;
# MAGIC > Αν τυχαία διαλέξετε 1 πραγματικά flagged δήλωση και 1 πραγματικά clean,
# MAGIC > το μοντέλο θα δώσει υψηλότερο score στη flagged με πιθανότητα 85%.
# MAGIC > **AUC > 0.7** = αξιοποιήσιμο **·** **AUC > 0.85** = πολύ καλό **·** **AUC ≈ 0.5** = άχρηστο.

# COMMAND ----------

import os
import logging
import urllib.request

# CRITICAL: Set MLFLOW_REGISTRY_URI BEFORE any mlflow import/call.
# Σε Spark Connect (Free Edition / Serverless), το MlflowClient() καλεί
# spark.conf.get('spark.mlflow.modelRegistryUri') και σπάει με
# CONFIG_NOT_AVAILABLE. Με το env var, το mlflow short-circuits και
# δεν φτάνει ποτέ στο spark.conf call.
os.environ["MLFLOW_REGISTRY_URI"] = "databricks-uc"

# Σιγάζουμε noisy GRPC warnings από Spark Connect probe failures.
# Σε Free Edition/Serverless, το spark.conf.get() για κάποια keys αποτυγχάνει
# με GRPC error, ο οποίος γίνεται handle εσωτερικά αλλά τυπώνεται στο output.
# Αυτές οι warnings δεν επηρεάζουν την εκτέλεση — απλώς θόρυβος.

# Level suppression για τους named loggers
for _name in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
              "pyspark", "py4j", "grpc"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)


# Filter που drop-άρει GRPC-related messages από handler level
# (Το setLevel μόνο δεν αρκεί γιατί τα messages προπαγάνδονται στους root handlers)
class _DropGrpcNoise(logging.Filter):
    def filter(self, record):
        msg = str(record.getMessage()) if record.args is None else record.msg
        return not (
            "GRPC Error received" in str(msg)
            or "_handle_rpc_error" == record.funcName
            or "rpc_error" in str(msg).lower()
            or "Config(req" in str(msg)
        )


_grpc_filter = _DropGrpcNoise()
# Apply σε ΟΛΟΥΣ τους handlers (root + named loggers)
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_grpc_filter)
# Apply και σε self level σε περίπτωση που νέοι handlers δημιουργηθούν αργότερα
for _name in ("pyspark.sql.connect.client.core", "pyspark.sql.connect",
              "pyspark", "py4j", "grpc", ""):
    logging.getLogger(_name).addFilter(_grpc_filter)

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

    # Log model — try modern API (MLflow 2.20+) πρώτα, fallback σε legacy
    try:
        mlflow.sklearn.log_model(sk_model=model, name="model")
    except TypeError:
        # MLflow < 2.20 χρησιμοποιεί artifact_path
        mlflow.sklearn.log_model(model, artifact_path="model")
    run_id = run.info.run_id

print(f"\n✓ Baseline trained. AUC = {auc:.4f}")
print(f"✓ Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🪜 Βήμα 1: Register στο MLflow Registry με tag 'staging'
# MAGIC
# MAGIC ### 📚 Τι είναι «Model Registry»;
# MAGIC > Κεντρικό **μητρώο** όλων των μοντέλων του οργανισμού — σαν «GitHub για μοντέλα».
# MAGIC > Κάθε register δίνει αυτόματα:
# MAGIC > - **Όνομα** (π.χ. `aade_risk_scorer`)
# MAGIC > - **Version** (1, 2, 3, …) — κάθε νέα εκδοχή κρατάει ιστορικότητα
# MAGIC > - **Stages/Aliases** (staging → production → archived)
# MAGIC > - **Tags** (key-value metadata, π.χ. `owner=ml_team`, `dataset=2024Q4`)
# MAGIC > - **Lineage**: ποιο run δημιούργησε το μοντέλο, ποιος το έγραψε, με ποια data
# MAGIC > - **Approval workflow**: ποιος είδε, ενέκρινε, archivάρισε
# MAGIC
# MAGIC ### ❓ Γιατί όχι απλώς να σώσω το model σε .pkl;
# MAGIC | Πρόβλημα με .pkl files | Πώς το λύνει το Registry |
# MAGIC |---|---|
# MAGIC | Ποιο .pkl είναι production; | Alias `production` σε ακριβώς ένα version |
# MAGIC | Ποιος το εκπαίδευσε; Με τι data; | Lineage tracking |
# MAGIC | Πώς γυρίζω rollback σε προηγούμενη version; | `set_alias` σε προηγούμενο version |
# MAGIC | Ποιος ενέκρινε το deployment; | Tags + audit log |
# MAGIC
# MAGIC ### 1α. Η ιδέα
# MAGIC
# MAGIC Το trained model ζει αυτή τη στιγμή σαν **artifact ενός run** (αρχείο στο
# MAGIC filesystem που σώθηκε με `mlflow.sklearn.log_model()`). Δεν μπορεί να βρεθεί
# MAGIC εύκολα από άλλη ομάδα. Δεν έχει version. Δεν έχει approval workflow.
# MAGIC
# MAGIC Με `mlflow.register_model()` γίνεται **catalog-level entity** με versioning,
# MAGIC stages, lineage και metadata.
# MAGIC
# MAGIC ### 1β. Το όνομα
# MAGIC
# MAGIC Στο Unity Catalog ο πλήρης δρόμος είναι `catalog.schema.model_name`
# MAGIC (3-part name — όπως ο πίνακας `workspace.aade.tax_features`).
# MAGIC Στην περίπτωσή μας: `workspace.aade.aade_risk_scorer`.
# MAGIC
# MAGIC ### 📚 Τι είναι «model URI»;
# MAGIC > String που δείχνει σε ένα model artifact. Φόρμες:
# MAGIC > - `runs:/<run_id>/model` — model του συγκεκριμένου run (πριν register)
# MAGIC > - `models:/<name>/<version>` — registered model, συγκεκριμένη version
# MAGIC > - `models:/<name>@production` — registered model, ό,τι έχει το alias `production` (UC)
# MAGIC > - `models:/<name>/Production` — legacy stage (non-UC workspaces)

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

# Explicit set του registry URI σε UC. Παρά το env var, μερικές εκδόσεις mlflow
# χρειάζονται το ρητό set_registry_uri για να ενημερωθεί το module state.
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

print(f"→ MLflow registry URI: {mlflow.get_registry_uri()}")
print(f"→ Tracking URI:        {mlflow.get_tracking_uri()}")
print(f"→ Target model name:   {model_name}")

# Register (ή skip αν registry not available)
mv = None
model_uri = f"runs:/{run_id}/model"
try:
    mv = mlflow.register_model(model_uri=model_uri, name=model_name)
    print(f"\n✓ Registered στο UC: {model_name} version {mv.version}")
except Exception as e:
    err_str = str(e)
    print(f"\n⚠️  UC registration απέτυχε: {type(e).__name__}")
    print(f"    Full error: {err_str[:500]}")
    # Δοκιμάζουμε legacy workspace registry
    try:
        os.environ["MLFLOW_REGISTRY_URI"] = "databricks"
        mlflow.set_registry_uri("databricks")
        client = MlflowClient(registry_uri="databricks")
        model_name = "aade_risk_scorer"
        USE_UC = False
        mv = mlflow.register_model(model_uri=model_uri, name=model_name)
        print(f"✓ Registered στο workspace registry: {model_name} version {mv.version}")
    except Exception as e2:
        USE_REGISTRY = False
        print(f"⚠️  Workspace registry απέτυχε: {type(e2).__name__}")
        print(f"    Full error: {str(e2)[:500]}")
        print("\n   Συνεχίζουμε σε in-run mode — model URI: runs:/run_id/model")
        print("   Πιθανές αιτίες:")
        print("   1. Δεν έχετε CREATE MODEL privilege στο schema workspace.aade")
        print("      → SQL: GRANT CREATE MODEL ON SCHEMA workspace.aade TO `<your_email>`")
        print("   2. Το workspace δεν έχει Unity Catalog enabled (Free Edition το έχει by default)")
        print("   3. Το DBR runtime του cluster δεν υποστηρίζει UC registry (χρειάζεται 13.0+)")

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
# MAGIC ### 📚 Τι είναι «alias»;
# MAGIC > Ονομασία που δείχνει σε συγκεκριμένη version του μοντέλου — σαν **git tag**
# MAGIC > ή **DNS CNAME**. Π.χ. `production` → version 5.
# MAGIC >
# MAGIC > **Πώς δουλεύει το rollback**: αν version 6 είναι buggy, αλλάζουμε τον alias να
# MAGIC > δείχνει σε version 5 — **χωρίς να αλλάξουμε γραμμή κώδικα** στα downstream
# MAGIC > pipelines που χρησιμοποιούν `models:/aade_risk_scorer@production`.
# MAGIC
# MAGIC ### 📚 Stages vs Aliases — ποια διαφορά;
# MAGIC | Παλιό σύστημα (workspace registry) | Νέο σύστημα (Unity Catalog) |
# MAGIC |---|---|
# MAGIC | Σταθερά stages: `None / Staging / Production / Archived` | Όποιο alias θέλετε: `production`, `champion`, `canary` |
# MAGIC | Μόνο 1 version σε Production ταυτόχρονα | Πολλά aliases ταυτόχρονα |
# MAGIC | `transition_model_version_stage()` | `set_registered_model_alias()` |
# MAGIC | `models:/name/Production` URI | `models:/name@production` URI |
# MAGIC
# MAGIC Τα aliases είναι **πιο ευέλικτα**: μπορείτε να έχετε `champion` για το βασικό μοντέλο και
# MAGIC `challenger` για A/B test ταυτόχρονα.
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
# MAGIC > Απαιτείται 2 approvals: data scientist + compliance officer (governance gate).

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
# MAGIC ### 📚 Τι είναι «batch scoring»;
# MAGIC > Παίρνουμε **πολλές εγγραφές μαζί** (batch) και τις σκοράρουμε σε ένα job —
# MAGIC > π.χ. όλες οι 5 εκατομμύρια ΑΦΜ της ΑΑΔΕ μία φορά τη νύχτα.
# MAGIC > Αντίθετο του **real-time scoring** (μία πρόβλεψη τη φορά μέσω REST API).
# MAGIC
# MAGIC ### 📚 Τι είναι «UDF» (User-Defined Function);
# MAGIC > Συνάρτηση που γράφεις εσύ και την εκτελεί ο Spark **παράλληλα** σε όλους τους
# MAGIC > workers. Παράδειγμα: αντί να φέρουμε όλο το dataset σε έναν node και να
# MAGIC > τρέξουμε το model σε for loop, κάθε worker σκοράρει το **κομμάτι** του.
# MAGIC > Σε 10-node cluster → 10× πιο γρήγορα.
# MAGIC
# MAGIC ### 📚 Τι κάνει το `mlflow.pyfunc.spark_udf`;
# MAGIC > Παίρνει το URI του μοντέλου, **κατεβάζει** το model σε όλους τους workers,
# MAGIC > και επιστρέφει UDF που εφαρμόζεται με `df.withColumn("score", udf(*cols))`.
# MAGIC > Δουλεύει με **οποιοδήποτε** mlflow model: sklearn, xgboost, pytorch, custom.
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
# MAGIC
# MAGIC ### 📚 Τι είναι «audit log» / «input hash»;
# MAGIC > Για κάθε prediction κρατάμε γραμμή στο `prediction_audit` table με:
# MAGIC > - **timestamp**: πότε σκοραρίστηκε
# MAGIC > - **model_version**: ποια version του μοντέλου χρησιμοποιήθηκε
# MAGIC > - **input_hash**: SHA-256 του input — αν αύριο ο πολίτης ισχυριστεί «δεν έβαλα
# MAGIC >   ποτέ αυτά τα δεδομένα», συγκρίνουμε hashes και αποδεικνύουμε
# MAGIC >   **non-repudiation** (μη αμφισβήτηση)
# MAGIC > - **risk_score**: το score που έδωσε
# MAGIC >
# MAGIC > **Νομική απαίτηση** για EU AI Act high-risk systems: retention 7+ χρόνια.

# COMMAND ----------

from pyspark.sql.functions import struct, current_timestamp, sha2, concat_ws, col, lit
from pyspark.sql.types import DoubleType

# Επιλογή URI ανάλογα με το mode
if not USE_REGISTRY:
    prod_model_uri = f"runs:/{run_id}/model"
elif USE_UC:
    prod_model_uri = f"models:/{model_name}@production"
else:
    prod_model_uri = f"models:/{model_name}/Production"

# Δοκιμάζουμε spark_udf πρώτα (production way).
# Αν αποτύχει (Spark Connect quirks σε Free Edition), fallback σε pandas.
USE_SPARK_UDF = False
predict_udf = None
loaded_pandas_model = None
try:
    predict_udf = mlflow.pyfunc.spark_udf(
        spark,
        model_uri=prod_model_uri,
        result_type=DoubleType(),
        env_manager="local",
    )
    USE_SPARK_UDF = True
    print(f"✓ Model loaded ως spark_udf: {prod_model_uri}")
except Exception as e:
    print(f"⚠️  spark_udf δεν είναι διαθέσιμο σε αυτό το environment: {type(e).__name__}")
    print("   Fallback σε pandas-based batch scoring.")
    try:
        loaded_pandas_model = mlflow.pyfunc.load_model(prod_model_uri)
    except Exception:
        loaded_pandas_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model")
    print(f"✓ Model loaded ως pandas pyfunc")

if USE_SPARK_UDF:
    # Production path: παραλληλοποιημένο scoring με Spark UDF
    features_sdf = spark.read.csv(local_path, header=True, inferSchema=True)
    scored = (
        features_sdf
        .withColumn("risk_score", predict_udf(struct(*[col(f) for f in features])))
        .withColumn("scored_at", current_timestamp())
        .withColumn("model_version", lit(str(mv.version)))
    )
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
else:
    # Free Edition path: pandas-based scoring, μετά Spark write
    import hashlib
    pdf = pd.read_csv(local_path)
    pdf_features = pdf[features].fillna(pdf[features].median(numeric_only=True))
    pdf["risk_score"] = loaded_pandas_model.predict(pdf_features)
    pdf["scored_at"] = pd.Timestamp.utcnow()
    pdf["model_version"] = str(mv.version)
    pdf["input_hash"] = pdf_features.apply(
        lambda r: hashlib.sha256("|".join(map(str, r.values)).encode()).hexdigest(),
        axis=1,
    )
    scored = spark.createDataFrame(pdf.drop(columns=["input_hash"]))
    afm_col = "afm" if "afm" in pdf.columns else features[0]
    audit = spark.createDataFrame(
        pdf[[afm_col, "scored_at", "model_version", "risk_score", "input_hash"]]
    )

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
# MAGIC ### 📚 Τι είναι «REST API» / «Endpoint»;
# MAGIC > Ένα **HTTP URL** που δέχεται POST requests με JSON input και επιστρέφει JSON output.
# MAGIC > Π.χ. `POST https://aade.gov.gr/api/score` με body `{"income": 50000}` →
# MAGIC > απάντηση `{"risk_score": 0.73}`.
# MAGIC >
# MAGIC > Tο **Databricks Model Serving** δημιουργεί αυτόματα ένα τέτοιο URL για το μοντέλο
# MAGIC > σας — με authentication, autoscaling, load balancing, monitoring «out of the box».
# MAGIC
# MAGIC ### 📚 Batch vs Real-time — πώς επιλέγουμε;
# MAGIC | Σενάριο | Batch ✓ | Real-time ✓ |
# MAGIC |---|---|---|
# MAGIC | Νυχτερινό scoring 5M ΑΦΜ για audit prioritization | ✅ | |
# MAGIC | Πολίτης υποβάλλει δήλωση στο TAXIS — χρειάζεται απάντηση σε <1s | | ✅ |
# MAGIC | Daily report στο τμήμα ελέγχων | ✅ | |
# MAGIC | Fraud detection σε ζωντανή συναλλαγή | | ✅ |
# MAGIC | **Κόστος ανά prediction** | Πολύ χαμηλό | Υψηλότερο (always-on cluster) |
# MAGIC | **Latency** | Λεπτά-ώρες | Δεκάδες ms |
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
# MAGIC ### 📚 Τι είναι «drift»;
# MAGIC > Όταν τα **πραγματικά δεδομένα σήμερα** είναι **διαφορετικά** από αυτά με τα οποία
# MAGIC > εκπαιδεύσαμε το μοντέλο. Παραδείγματα από ΑΑΔΕ:
# MAGIC > - **Πληθωρισμός 25%** → όλα τα incomes ανέβηκαν → το μοντέλο που έμαθε «income > 60k = high»
# MAGIC >   τώρα flagάρει υπερβολικά πολύ κόσμο
# MAGIC > - **Νέος φορολογικός νόμος** → άλλαξε ο tax_rate διανομής
# MAGIC > - **COVID/πανδημία** → ξαφνική πτώση εισοδημάτων στους ΚΑΔ τουρισμού
# MAGIC
# MAGIC ### 📚 Δύο τύποι drift
# MAGIC | Τύπος | Τι αλλάζει | Πώς το πιάνουμε |
# MAGIC |---|---|---|
# MAGIC | **Data drift (covariate shift)** | Αλλάζει η **κατανομή των features** | PSI / KS test (αυτό το lab) |
# MAGIC | **Concept drift** | Αλλάζει η **σχέση features → target** | Παρακολουθούμε AUC/precision όταν έχουμε labels |
# MAGIC | **Label drift** | Αλλάζει η **κατανομή του target** | Παρακολουθούμε class balance |
# MAGIC
# MAGIC ### 📚 Τι είναι «PSI» (Population Stability Index);
# MAGIC > Αριθμός που μετράει **πόσο διαφέρει** η σημερινή κατανομή ενός feature από τη baseline.
# MAGIC > **Πώς υπολογίζεται:**
# MAGIC > 1. Χωρίζουμε το feature σε bins (π.χ. 10 «κουτιά» income brackets)
# MAGIC > 2. Υπολογίζουμε ποσοστό σε κάθε bin για baseline (`b%`) και current (`c%`)
# MAGIC > 3. PSI = Σ (c% − b%) × ln(c% / b%) σε όλα τα bins
# MAGIC >
# MAGIC > **Ερμηνεία**: **PSI < 0.1** σταθερό ✅ **·** **0.1-0.2** watch ⚠️ **·** **> 0.2** retrain 🚨
# MAGIC
# MAGIC ### 📚 Τι είναι «KS test» (Kolmogorov-Smirnov);
# MAGIC > Στατιστικός έλεγχος που λέει αν δύο δείγματα προέρχονται από **ίδια κατανομή**.
# MAGIC > Επιστρέφει:
# MAGIC > - **statistic** (D): η μέγιστη απόσταση μεταξύ των δύο **CDFs** (cumulative distributions)
# MAGIC > - **p-value**: αν είναι **< 0.05** → reject «είναι ίδια κατανομή» → **drift detected**
# MAGIC >
# MAGIC > Διαφορά από PSI: το KS δίνει **στατιστική σημαντικότητα** (αν διαφέρουν αληθινά),
# MAGIC > ενώ το PSI δίνει **μέγεθος διαφοράς**. Καλό να βλέπουμε **και τα δύο**.
# MAGIC
# MAGIC ### 📚 Τι είναι «baseline» / «retraining»;
# MAGIC > **Baseline** = η κατανομή των features στο training set — αυτό το «ξέρει» το μοντέλο.
# MAGIC > Πρέπει να την **παγώσουμε** σε Delta table την ημέρα του deployment.
# MAGIC >
# MAGIC > **Retraining** = όταν drift > threshold, ξανατρέχουμε όλο το training pipeline
# MAGIC > με τα νέα data ώστε το μοντέλο να «μάθει» τη νέα πραγματικότητα.
# MAGIC > Συνήθως αυτοματοποιημένο σε CI/CD pipeline.
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
# MAGIC ### 📚 Τι είναι το «EU AI Act»;
# MAGIC > Ευρωπαϊκός κανονισμός (σε ισχύ από 2024-2026 σταδιακά) που κατηγοριοποιεί τα AI
# MAGIC > συστήματα σε **4 επίπεδα κινδύνου**:
# MAGIC > - **Unacceptable risk** (απαγορευμένα): social scoring, real-time biometric surveillance σε public space
# MAGIC > - **High risk**: μοντέλα που **επηρεάζουν θεμελιώδη δικαιώματα** — π.χ. πρόσβαση σε εκπαίδευση,
# MAGIC >   εργασία, δάνεια, **φορολογικός έλεγχος**, social benefits, justice
# MAGIC > - **Limited risk**: chatbots, deepfakes — απαιτούν transparency disclosure
# MAGIC > - **Minimal risk**: spam filters, video games — καμία υποχρέωση
# MAGIC >
# MAGIC > Το μοντέλο της ΑΑΔΕ είναι **High Risk** → πλήρης compliance υποχρέωση.
# MAGIC
# MAGIC ### 📚 Τι απαιτεί το EU AI Act για High-Risk συστήματα;
# MAGIC | Άρθρο | Απαίτηση | Πώς το ικανοποιούμε στο lab |
# MAGIC |---|---|---|
# MAGIC | Art. 12 | **Audit logging** όλων των predictions | `prediction_audit` table με input_hash + timestamp |
# MAGIC | Art. 13 | **Transparency** — εξήγηση κάθε απόφασης | SHAP top-3 features ανά flagged prediction |
# MAGIC | Art. 14 | **Human oversight** — ποτέ auto-action για high-risk | review queue για score > 0.8 |
# MAGIC | Art. 15 | **Robustness** — drift monitoring & retraining | PSI/KS daily checks (Βήμα 5) |
# MAGIC | Art. 17 | **Quality management system** — version control, lineage | MLflow Registry (Βήμα 1) |
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
# MAGIC ### 📚 Τι είναι «human-in-the-loop» (HITL);
# MAGIC > Pattern όπου **άνθρωπος εγκρίνει** την τελική απόφαση πριν εκτελεστεί. Το AI
# MAGIC > απλώς **προτείνει**. Π.χ. το μοντέλο λέει «ύποπτη δήλωση score=0.92» αλλά
# MAGIC > **ελεγκτής** βλέπει την υπόθεση, εξετάζει context (π.χ. ο φορολογούμενος είχε
# MAGIC > σοβαρό ατύχημα), και αποφασίζει αν θα γίνει έλεγχος.
# MAGIC >
# MAGIC > **Όχι automation σε high-risk decisions** — άρθρο 14 EU AI Act.
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
# MAGIC ### 📚 Τι είναι «SHAP»;
# MAGIC > **SH**apley **A**dditive ex**P**lanations — μέθοδος που εξηγεί **γιατί** το μοντέλο
# MAGIC > έδωσε συγκεκριμένο score σε μία γραμμή. Βασίζεται στη **θεωρία παιγνίων** του
# MAGIC > Lloyd Shapley (Νόμπελ Οικονομικών 2012).
# MAGIC >
# MAGIC > **Τι κάνει**: «μοιράζει» το score σε **συνεισφορές ανά feature**. Παράδειγμα:
# MAGIC > ```
# MAGIC > Prediction: 0.87 (high risk)
# MAGIC > Baseline (μέσος όρος όλων): 0.30
# MAGIC > → Σύνολο: 0.30 + 0.57 = 0.87
# MAGIC > Συνεισφορές:
# MAGIC >   ↑ income_change_yoy = -65%   → +0.32 (μεγάλη πτώση εισοδήματος)
# MAGIC >   ↑ expense_ratio = 95%        → +0.18 (πολύ ψηλά έξοδα)
# MAGIC >   ↑ declaration_count = 1      → +0.07 (πρώτη δήλωση)
# MAGIC > ```
# MAGIC > Δηλαδή το μοντέλο **flagάρει** αυτή τη δήλωση **κυρίως** λόγω της ξαφνικής πτώσης
# MAGIC > εισοδήματος και των φουσκωμένων εξόδων.
# MAGIC
# MAGIC ### 📚 Τι είναι «TreeExplainer»;
# MAGIC > Γρήγορη εκδοχή του SHAP **ειδικά για tree-based μοντέλα** (RandomForest, XGBoost, LightGBM).
# MAGIC > Υπολογίζει τα ακριβή Shapley values σε O(TLD²) αντί για εκθετικό χρόνο των
# MAGIC > generic explainers. Για 1 prediction: λίγα ms.
# MAGIC
# MAGIC ### 🎯 Πρακτικό σενάριο
# MAGIC Όταν ο πολίτης ρωτήσει «γιατί ελέγχθηκα;», η ΑΑΔΕ πρέπει να απαντήσει με
# MAGIC **κατανοητή εξήγηση** — όχι «έτσι αποφάσισε ο αλγόριθμος». Με SHAP, για κάθε
# MAGIC prediction παίρνουμε τα **top 3 features** που οδήγησαν στο score, σε μορφή που
# MAGIC ένας ελεγκτής μπορεί να εξηγήσει σε φυσική γλώσσα.

# COMMAND ----------

# Εγκατάσταση SHAP αν δεν υπάρχει ήδη
try:
    import shap
except ImportError:
    print("Εγκατάσταση SHAP (μπορεί να χρειαστούν λίγα δευτερόλεπτα)...")
    import subprocess
    subprocess.check_call(["pip", "install", "-q", "shap"])
    import shap

import numpy as np

# SHAP για ένα flagged sample
explainer = shap.TreeExplainer(model)
sample = X_test.head(1)
shap_values = explainer.shap_values(sample)

# Normalize σε 1D array για το single sample, positive class.
# RandomForestClassifier binary: shap_values μπορεί να είναι:
#  - list of 2 arrays (κάθε μία shape (1, n_features)) — old API
#  - ndarray shape (1, n_features, 2) — new API
#  - ndarray shape (1, n_features) — single class
arr = np.asarray(shap_values)
if isinstance(shap_values, list):
    shap_vals = np.asarray(shap_values[1])[0]  # positive class, first sample
elif arr.ndim == 3:
    shap_vals = arr[0, :, 1]  # first sample, positive class
elif arr.ndim == 2:
    shap_vals = arr[0]  # first sample
else:
    shap_vals = arr.flatten()

# Top 3 contributing features
contributions = sorted(
    zip(features, [float(v) for v in shap_vals]),
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
# MAGIC ### 📖 Συνολικό Glossary του Lab
# MAGIC
# MAGIC | Όρος | Σύντομος ορισμός |
# MAGIC |---|---|
# MAGIC | **MLflow** | Open-source πλατφόρμα για tracking, registry, deployment μοντέλων |
# MAGIC | **Run** | Μία εκτέλεση training — αποθηκεύει params, metrics, artifacts |
# MAGIC | **Artifact** | Αρχείο σχετικό με run (model.pkl, plots, eval reports) |
# MAGIC | **Registry** | Κεντρικό μητρώο μοντέλων με versioning + approval workflow |
# MAGIC | **Model URI** | String που δείχνει σε model: `runs:/.../model`, `models:/name@alias` |
# MAGIC | **Alias** | Ονομασία που δείχνει σε version (UC) — modern replacement των stages |
# MAGIC | **Stage** | Παλιό σύστημα: None/Staging/Production/Archived (workspace registry) |
# MAGIC | **Lineage** | Τι data + code παρήγαγε το μοντέλο — για audit & reproducibility |
# MAGIC | **AUC** | Area Under ROC Curve — μετρική 0.5-1.0 για binary classification |
# MAGIC | **Spark UDF** | Συνάρτηση που εκτελείται παράλληλα σε όλους τους Spark workers |
# MAGIC | **Batch scoring** | Scoring πολλών εγγραφών μαζί σε scheduled job |
# MAGIC | **Real-time scoring** | Scoring 1 εγγραφής τη φορά μέσω REST API |
# MAGIC | **Endpoint** | HTTP URL που εξυπηρετεί predictions (Databricks Model Serving) |
# MAGIC | **Audit log** | Πίνακας με κάθε prediction + timestamp + input_hash |
# MAGIC | **Input hash** | SHA-256 του input — για non-repudiation σε νομικές αμφισβητήσεις |
# MAGIC | **Drift** | Όταν τα data σήμερα διαφέρουν από τα training data |
# MAGIC | **Data drift** | Αλλάζει η κατανομή των features (covariate shift) |
# MAGIC | **Concept drift** | Αλλάζει η σχέση features → target |
# MAGIC | **PSI** | Population Stability Index — μέγεθος απόκλισης κατανομών |
# MAGIC | **KS test** | Kolmogorov-Smirnov — στατιστική σημαντικότητα διαφοράς |
# MAGIC | **Baseline** | Παγωμένη κατανομή του training set για drift comparison |
# MAGIC | **Retraining** | Ξανατρέχουμε training όταν drift > threshold |
# MAGIC | **HITL** | Human-in-the-loop — άνθρωπος εγκρίνει κρίσιμες αποφάσεις |
# MAGIC | **SHAP** | Shapley values — εξήγηση συνεισφοράς κάθε feature στο score |
# MAGIC | **TreeExplainer** | Γρήγορος SHAP explainer για tree-based μοντέλα |
# MAGIC | **EU AI Act** | Ευρωπαϊκός κανονισμός για AI — high-risk = αυστηρές υποχρεώσεις |
# MAGIC | **Non-repudiation** | Νομική αρχή: ο εκδότης δεν μπορεί να αρνηθεί την ενέργεια |
# MAGIC | **Champion/Challenger** | A/B test pattern: production model vs νέο για σύγκριση |
# MAGIC
# MAGIC ### 🧠 Mental model: τα 5 βήματα μαζί
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐    ┌──────────┐    ┌────────────┐    ┌──────────────┐    ┌──────────┐
# MAGIC │ Trained     │──▶│ Registry │──▶│ Production │──▶│ Batch Score  │──▶│ Audit /  │
# MAGIC │ Model (run) │    │ (v1)     │    │ Alias      │    │ + REST API   │    │ Drift /  │
# MAGIC │             │    │          │    │            │    │              │    │ SHAP     │
# MAGIC └─────────────┘    └──────────┘    └────────────┘    └──────────────┘    └──────────┘
# MAGIC      Step 0           Step 1          Step 2          Steps 3 & 4         Steps 5 & 6
# MAGIC ```
# MAGIC
# MAGIC ### 💡 Take-home message
# MAGIC
# MAGIC > **Ένα μοντέλο σε notebook δεν λύνει κανένα πρόβλημα.
# MAGIC > Ένα μοντέλο σε production με registry, monitoring, audit trail και explainability
# MAGIC > λύνει πραγματικά προβλήματα — και είναι νόμιμο.**
# MAGIC >
# MAGIC > Αυτή η ροή είναι **το minimum** για να βγει ML σύστημα ΑΑΔΕ σε production μετά το 2027,
# MAGIC > σύμφωνα με την EU AI Act.
