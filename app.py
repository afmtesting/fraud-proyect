import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

st.set_page_config(page_title="Fraud Project Dashboard", layout="wide")

st.title("Fraud Project Dashboard")

pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)

# =========================
# Cargar datos
# =========================
kpi = pd.read_sql("select * from public.mart_fraud_kpi limit 1", engine)
channel = pd.read_sql("select * from public.mart_fraud_by_channel", engine)
metrics = pd.read_sql("select * from public.ml_model_metrics", engine)

# =========================
# KPIs
# =========================
st.header("KPIs")

if not kpi.empty:
    col1, col2, col3, col4 = st.columns(4)

    tx_count = int(kpi["tx_count"].iloc[0]) if "tx_count" in kpi.columns else 0
    fraud_tx_count = int(kpi["fraud_tx_count"].iloc[0]) if "fraud_tx_count" in kpi.columns else 0
    fraud_rate = float(kpi["fraud_rate"].iloc[0]) if "fraud_rate" in kpi.columns else 0
    amount_total = float(kpi["amount_total"].iloc[0]) if "amount_total" in kpi.columns else 0

    col1.metric("Total Transacciones", f"{tx_count:,}")
    col2.metric("Transacciones Fraude", f"{fraud_tx_count:,}")
    col3.metric("Tasa de Fraude", f"{fraud_rate:.4f}")
    col4.metric("Monto Total", f"{amount_total:,.2f}")
else:
    st.warning("No hay datos en public.mart_fraud_kpi")

# =========================
# Gráfica: fraude por canal
# =========================
st.header("Fraude por Canal")

if not channel.empty:
    st.dataframe(channel, use_container_width=True)

    if "channel" in channel.columns and "fraud_tx_count" in channel.columns:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(channel["channel"], channel["fraud_tx_count"])
        ax.set_title("Transacciones de fraude por canal")
        ax.set_xlabel("Canal")
        ax.set_ylabel("Fraudes")
        st.pyplot(fig)

    if "channel" in channel.columns and "fraud_rate" in channel.columns:
        fig2, ax2 = plt.subplots(figsize=(8, 4))
        ax2.bar(channel["channel"], channel["fraud_rate"])
        ax2.set_title("Tasa de fraude por canal")
        ax2.set_xlabel("Canal")
        ax2.set_ylabel("Fraud Rate")
        st.pyplot(fig2)
else:
    st.warning("No hay datos en public.mart_fraud_by_channel")

# =========================
# Modelo ML
# =========================
st.header("Modelo de Machine Learning")

if not metrics.empty:
    st.write("El modelo fue entrenado y sus métricas quedaron registradas en la tabla public.ml_model_metrics.")

    st.dataframe(metrics, use_container_width=True)

    metric_cols = st.columns(len(metrics.columns))
    for i, col_name in enumerate(metrics.columns):
        try:
            value = float(metrics[col_name].iloc[0])
            metric_cols[i].metric(col_name, f"{value:.4f}")
        except Exception:
            metric_cols[i].metric(col_name, str(metrics[col_name].iloc[0]))

    numeric_cols = metrics.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        fig3, ax3 = plt.subplots(figsize=(8, 4))
        ax3.bar(numeric_cols, [float(metrics[c].iloc[0]) for c in numeric_cols])
        ax3.set_title("Métricas del modelo")
        ax3.set_ylabel("Valor")
        plt.xticks(rotation=45)
        st.pyplot(fig3)
else:
    st.warning("No hay datos en public.ml_model_metrics")

# =========================
# Evidencia del modelo
# =========================
st.header("Evidencia de entrenamiento")

st.markdown(
    """
- El modelo se entrenó con el script `ml/train_model.py`
- El entrenamiento generó métricas persistidas en `public.ml_model_metrics`
- Localmente también se generó el artefacto `artifacts/model.joblib`
"""
)