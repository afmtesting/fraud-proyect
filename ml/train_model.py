import os
import json
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
    average_precision_score
)

load_dotenv()

pg_url = os.getenv("PG_URL")
if not pg_url:
    raise ValueError("No se encontró PG_URL en el archivo .env o secrets.")

engine = create_engine(pg_url)

# =========================
# Carga de datos
# =========================
df = pd.read_sql("select * from public.fact_transactions", engine)

# Variable objetivo derivada del mart actual
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
    "fraud_rate",
]

X = df.drop(columns=[c for c in drop_cols if c in df.columns] + [target], errors="ignore")
y = df[target]

X = X.select_dtypes(include=["number"]).fillna(0)

if y.nunique() < 2:
    raise ValueError("La variable objetivo no tiene al menos dos clases; no se puede entrenar el modelo.")

# =========================
# Split
# =========================
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
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
    class_weight="balanced",
    n_jobs=-1
)
model.fit(X_train, y_train)

# =========================
# Predicciones base
# =========================
y_pred_default = model.predict(X_test)
y_score = model.predict_proba(X_test)[:, 1]

accuracy = accuracy_score(y_test, y_pred_default)
precision = precision_score(y_test, y_pred_default, zero_division=0)
recall = recall_score(y_test, y_pred_default, zero_division=0)
f1 = f1_score(y_test, y_pred_default, zero_division=0)
pr_auc = average_precision_score(y_test, y_score)

# =========================
# Guardar artefacto local
# =========================
os.makedirs("artifacts", exist_ok=True)
joblib.dump(model, "artifacts/model.joblib")

# =========================
# Persistir métricas del modelo
# =========================
metrics_df = pd.DataFrame([{
    "accuracy": accuracy,
    "precision": precision,
    "recall": recall,
    "f1_score": f1,
    "pr_auc": pr_auc,
    "modelo": "RandomForestClassifier",
    "threshold_default": 0.5,
    "n_train": len(X_train),
    "n_test": len(X_test)
}])

metrics_df.to_sql(
    "ml_model_metrics",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

# =========================
# Persistir predicciones de evaluación
# =========================
eval_df = pd.DataFrame({
    "y_true": y_test.reset_index(drop=True).astype(int),
    "y_score": pd.Series(y_score).astype(float),
    "y_pred_default": pd.Series(y_pred_default).astype(int)
})

eval_df.to_sql(
    "ml_eval_predictions",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

# =========================
# Persistir importancias de variables
# =========================
feature_importance_df = pd.DataFrame({
    "feature": X.columns,
    "importance": model.feature_importances_
}).sort_values("importance", ascending=False)

feature_importance_df.to_sql(
    "ml_feature_importance",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Modelo entrenado correctamente")
print(json.dumps({
    "accuracy": round(float(accuracy), 4),
    "precision": round(float(precision), 4),
    "recall": round(float(recall), 4),
    "f1_score": round(float(f1), 4),
    "pr_auc": round(float(pr_auc), 4)
}, indent=2))