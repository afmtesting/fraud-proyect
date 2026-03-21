import os
import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from dotenv import load_dotenv
import joblib

load_dotenv()

pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)

df = pd.read_sql("select * from public.fact_transactions limit 10000", engine)

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

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

preds = model.predict(X_test)
acc = accuracy_score(y_test, preds)

os.makedirs("artifacts", exist_ok=True)
joblib.dump(model, "artifacts/model.joblib")

metrics = pd.DataFrame([{"accuracy": acc}])
metrics.to_sql(
    "ml_model_metrics",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Model trained. Accuracy:", acc)