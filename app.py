import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Fraud Project Dashboard", layout="wide")

st.title("Fraud Project Dashboard")

pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)

# =========================
# KPIs
# =========================
st.header("KPIs")

kpi = pd.read_sql("select * from public.mart_fraud_kpi limit 1", engine)

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
# Fraude por canal
# =========================
st.header("Fraude por Canal")

channel = pd.read_sql("select * from public.mart_fraud_by_channel", engine)

if not channel.empty:
    st.dataframe(channel, use_container_width=True)
else:
    st.warning("No hay datos en public.mart_fraud_by_channel")

# =========================
# Métricas del modelo
# =========================
st.header("Métricas del Modelo")

metrics = pd.read_sql("select * from public.ml_model_metrics", engine)

if not metrics.empty:
    st.dataframe(metrics, use_container_width=True)
else:
    st.warning("No hay datos en public.ml_model_metrics")

# =========================
# Vista adicional
# =========================
st.header("Vista general de KPIs")

if not kpi.empty:
    st.dataframe(kpi, use_container_width=True)