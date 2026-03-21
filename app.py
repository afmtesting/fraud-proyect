import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

st.set_page_config(
    page_title="Fraud Analytics Dashboard",
    layout="wide"
)

pg_url = os.getenv("PG_URL")
engine = create_engine(pg_url)

# =========================
# Load data
# =========================
kpi = pd.read_sql("select * from public.mart_fraud_kpi limit 1", engine)
channel = pd.read_sql("select * from public.mart_fraud_by_channel", engine)
metrics = pd.read_sql("select * from public.ml_model_metrics", engine)

# =========================
# Header
# =========================
st.title("Fraud Analytics Dashboard")
st.caption("Prototype for fraud monitoring, analytics, and ML evaluation")

st.markdown("---")

# =========================
# KPI cards
# =========================
st.subheader("Executive KPIs")

if not kpi.empty:
    tx_count = int(kpi["tx_count"].iloc[0]) if "tx_count" in kpi.columns else 0
    fraud_tx_count = int(kpi["fraud_tx_count"].iloc[0]) if "fraud_tx_count" in kpi.columns else 0
    fraud_rate = float(kpi["fraud_rate"].iloc[0]) if "fraud_rate" in kpi.columns else 0
    amount_total = float(kpi["amount_total"].iloc[0]) if "amount_total" in kpi.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Transactions", f"{tx_count:,}")
    c2.metric("Fraud Transactions", f"{fraud_tx_count:,}")
    c3.metric("Fraud Rate", f"{fraud_rate:.4f}")
    c4.metric("Total Amount", f"{amount_total:,.2f}")
else:
    st.warning("No KPI data available")

st.markdown("---")

# =========================
# Charts row
# =========================
st.subheader("Fraud Monitoring")

left, right = st.columns(2)

with left:
    st.markdown("**Fraud Transactions by Channel**")
    if not channel.empty and "channel" in channel.columns and "fraud_tx_count" in channel.columns:
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(channel["channel"], channel["fraud_tx_count"])
        ax.set_xlabel("Channel")
        ax.set_ylabel("Fraud Transactions")
        ax.set_title("Fraud Volume")
        st.pyplot(fig, use_container_width=True)
    else:
        st.info("No channel fraud volume data available")

with right:
    st.markdown("**Fraud Rate by Channel**")
    if not channel.empty and "channel" in channel.columns and "fraud_rate" in channel.columns:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        ax2.bar(channel["channel"], channel["fraud_rate"])
        ax2.set_xlabel("Channel")
        ax2.set_ylabel("Fraud Rate")
        ax2.set_title("Fraud Rate")
        st.pyplot(fig2, use_container_width=True)
    else:
        st.info("No channel fraud rate data available")

st.markdown("---")

# =========================
# ML section
# =========================
st.subheader("Machine Learning Model")

if not metrics.empty:
    st.caption("Model metrics persisted from ml/train_model.py into public.ml_model_metrics")

    metric_columns = metrics.columns.tolist()
    metric_row = st.columns(min(len(metric_columns), 4))

    for i, col_name in enumerate(metric_columns[:4]):
        try:
            value = float(metrics[col_name].iloc[0])
            metric_row[i].metric(col_name.upper(), f"{value:.4f}")
        except Exception:
            metric_row[i].metric(col_name.upper(), str(metrics[col_name].iloc[0]))

    numeric_cols = metrics.select_dtypes(include=["number"]).columns.tolist()

    if numeric_cols:
        st.markdown("**Model Performance Overview**")
        fig3, ax3 = plt.subplots(figsize=(7, 4))
        ax3.bar(numeric_cols, [float(metrics[c].iloc[0]) for c in numeric_cols])
        ax3.set_ylabel("Metric Value")
        ax3.set_title("ML Metrics")
        plt.xticks(rotation=30)
        st.pyplot(fig3, use_container_width=False)
else:
    st.warning("No ML metrics available")

st.markdown("---")

# =========================
# Evidence / details
# =========================
st.subheader("Model Evidence")

st.markdown(
    """
- Training script: `ml/train_model.py`
- Metrics table in database: `public.ml_model_metrics`
- Local artifact generated during training: `artifacts/model.joblib`
"""
)

# =========================
# Optional detail tables
# =========================
with st.expander("View KPI table"):
    if not kpi.empty:
        st.dataframe(kpi, use_container_width=True)

with st.expander("View channel table"):
    if not channel.empty:
        st.dataframe(channel, use_container_width=True)

with st.expander("View ML metrics table"):
    if not metrics.empty:
        st.dataframe(metrics, use_container_width=True)