import os
import joblib
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    precision_recall_curve,
    average_precision_score
)

load_dotenv()

pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)

# =========================
# Cargar datos
# =========================
df = pd.read_sql("select * from public.fact_transactions limit 10000", engine)

# Variable objetivo derivada
df["is_fraud"] = (df["fraud_tx_count"] > 0).astype(int)

target = "is_fraud"

drop_cols = [
    "transaction_id",
    "dt",
    "transaction_ts",
    "channel",
    "device",
    "merchant",
    "fraud_tx_count",
    "fraud_rate"
]

X = df.drop(columns=[c for c in drop_cols if c in df.columns] + [target], errors="ignore")
y = df[target]

X = X.select_dtypes(include=["number"]).fillna(0)

# =========================
# Split
# =========================
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# =========================
# Entrenamiento
# =========================
model = RandomForestClassifier(
    random_state=42,
    n_estimators=200,
    class_weight="balanced"
)
model.fit(X_train, y_train)

# =========================
# Predicciones
# =========================
y_pred = model.predict(X_test)
y_score = model.predict_proba(X_test)[:, 1]

accuracy = accuracy_score(y_test, y_pred)
precision = precision_score(y_test, y_pred, zero_division=0)
recall = recall_score(y_test, y_pred, zero_division=0)
f1 = f1_score(y_test, y_pred, zero_division=0)
pr_auc = average_precision_score(y_test, y_score)

# =========================
# Guardar artefacto
# =========================
os.makedirs("artifacts", exist_ok=True)
joblib.dump(model, "artifacts/model.joblib")

# =========================
# Guardar métricas en DB
# =========================
metrics_df = pd.DataFrame([{
    "accuracy": accuracy,
    "precision": precision,
    "recall": recall,
    "f1_score": f1,
    "pr_auc": pr_auc
}])

metrics_df.to_sql(
    "ml_model_metrics",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

# =========================
# Guardar predicciones de evaluación
# =========================
eval_df = pd.DataFrame({
    "y_true": y_test.reset_index(drop=True),
    "y_score": y_score,
    "y_pred_default": y_pred
})

eval_df.to_sql(
    "ml_eval_predictions",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Modelo entrenado correctamente")
print(f"Accuracy : {accuracy:.4f}")
print(f"Precision: {precision:.4f}")
print(f"Recall   : {recall:.4f}")
print(f"F1-Score : {f1:.4f}")
print(f"PR AUC   : {pr_auc:.4f}")