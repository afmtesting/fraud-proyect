import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

st.set_page_config(
    page_title="Fraud Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =========================
# CSS
# =========================
st.markdown(
    """
    <style>
        .main {
            background-color: #0f172a;
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 2rem;
            max-width: 1400px;
        }

        h1, h2, h3, h4, h5, h6, p, div, span, label {
            color: #e5e7eb;
        }

        .dashboard-card {
            background: linear-gradient(135deg, #111827 0%, #1f2937 100%);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 18px;
            padding: 18px 20px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.25);
            margin-bottom: 10px;
        }

        .section-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #f9fafb;
            margin-bottom: 0.7rem;
        }

        .section-subtitle {
            font-size: 0.95rem;
            color: #9ca3af;
            margin-bottom: 1rem;
        }

        .metric-label {
            font-size: 0.9rem;
            color: #9ca3af;
            margin-bottom: 0.2rem;
        }

        .metric-value {
            font-size: 2rem;
            font-weight: 800;
            color: #ffffff;
            line-height: 1.1;
        }

        .metric-accent {
            height: 4px;
            width: 52px;
            border-radius: 999px;
            background: #38bdf8;
            margin-bottom: 12px;
        }

        div[data-testid="stDataFrame"] {
            border-radius: 14px;
            overflow: hidden;
        }

        div[data-testid="stExpander"] details {
            background: #111827;
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px;
        }

        div[data-testid="stMetric"] {
            background: transparent;
            border: none;
            padding: 0;
        }

        hr {
            border: none;
            height: 1px;
            background: rgba(255,255,255,0.08);
            margin: 1rem 0 1.2rem 0;
        }
    </style>
    """,
    unsafe_allow_html=True
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
# Helper
# =========================
def metric_card(title: str, value: str):
    st.markdown(
        f"""
        <div class="dashboard-card">
            <div class="metric-accent"></div>
            <div class="metric-label">{title}</div>
            <div class="metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

# =========================
# Header
# =========================
st.markdown(
    """
    <div class="dashboard-card">
        <div class="section-title" style="font-size: 2rem; margin-bottom: 0.3rem;">
            Fraud Analytics Dashboard
        </div>
        <div class="section-subtitle">
            Prototype for fraud monitoring, dimensional analytics, and machine learning evaluation
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# =========================
# KPI cards
# =========================
st.markdown(
    """
    <div class="section-title">Executive Overview</div>
    <div class="section-subtitle">Main indicators generated from the analytical mart</div>
    """,
    unsafe_allow_html=True
)

if not kpi.empty:
    tx_count = int(kpi["tx_count"].iloc[0]) if "tx_count" in kpi.columns else 0
    fraud_tx_count = int(kpi["fraud_tx_count"].iloc[0]) if "fraud_tx_count" in kpi.columns else 0
    fraud_rate = float(kpi["fraud_rate"].iloc[0]) if "fraud_rate" in kpi.columns else 0
    amount_total = float(kpi["amount_total"].iloc[0]) if "amount_total" in kpi.columns else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        metric_card("Transactions", f"{tx_count:,}")
    with c2:
        metric_card("Fraud Transactions", f"{fraud_tx_count:,}")
    with c3:
        metric_card("Fraud Rate", f"{fraud_rate:.4f}")
    with c4:
        metric_card("Total Amount", f"{amount_total:,.2f}")
else:
    st.warning("No KPI data available")

st.markdown("<hr>", unsafe_allow_html=True)

# =========================
# Charts
# =========================
st.markdown(
    """
    <div class="section-title">Fraud Monitoring by Channel</div>
    <div class="section-subtitle">Comparative view of fraud volume and fraud rate</div>
    """,
    unsafe_allow_html=True
)

left, right = st.columns(2)

with left:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown("**Fraud Transactions by Channel**")

    if not channel.empty and "channel" in channel.columns and "fraud_tx_count" in channel.columns:
        fig, ax = plt.subplots(figsize=(6, 3.8))
        ax.bar(channel["channel"], channel["fraud_tx_count"])
        ax.set_title("Fraud Volume", pad=12)
        ax.set_xlabel("Channel")
        ax.set_ylabel("Fraud Transactions")
        ax.grid(axis="y", alpha=0.25)
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)
    else:
        st.info("No channel fraud volume data available")
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown("**Fraud Rate by Channel**")

    if not channel.empty and "channel" in channel.columns and "fraud_rate" in channel.columns:
        fig2, ax2 = plt.subplots(figsize=(6, 3.8))
        ax2.bar(channel["channel"], channel["fraud_rate"])
        ax2.set_title("Fraud Rate", pad=12)
        ax2.set_xlabel("Channel")
        ax2.set_ylabel("Rate")
        ax2.grid(axis="y", alpha=0.25)
        st.pyplot(fig2, use_container_width=True)
        plt.close(fig2)
    else:
        st.info("No channel fraud rate data available")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =========================
# ML section
# =========================
st.markdown(
    """
    <div class="section-title">Machine Learning Model</div>
    <div class="section-subtitle">Persisted metrics from the training process executed by ml/train_model.py</div>
    """,
    unsafe_allow_html=True
)

ml_left, ml_right = st.columns([1.2, 1.8])

with ml_left:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)

    if not metrics.empty:
        metric_columns = metrics.columns.tolist()
        display_cols = metric_columns[:4]

        cols = st.columns(len(display_cols))
        for i, col_name in enumerate(display_cols):
            try:
                value = float(metrics[col_name].iloc[0])
                cols[i].metric(col_name.upper(), f"{value:.4f}")
            except Exception:
                cols[i].metric(col_name.upper(), str(metrics[col_name].iloc[0]))

        st.markdown(
            """
            **Model evidence**
            - Training script: `ml/train_model.py`
            - Persisted metrics: `public.ml_model_metrics`
            - Local artifact: `artifacts/model.joblib`
            """
        )
    else:
        st.warning("No ML metrics available")

    st.markdown('</div>', unsafe_allow_html=True)

with ml_right:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)

    if not metrics.empty:
        numeric_cols = metrics.select_dtypes(include=["number"]).columns.tolist()

        if numeric_cols:
            fig3, ax3 = plt.subplots(figsize=(7, 3.8))
            ax3.bar(numeric_cols, [float(metrics[c].iloc[0]) for c in numeric_cols])
            ax3.set_title("ML Performance Overview", pad=12)
            ax3.set_ylabel("Metric Value")
            ax3.grid(axis="y", alpha=0.25)
            plt.xticks(rotation=25)
            st.pyplot(fig3, use_container_width=True)
            plt.close(fig3)
        else:
            st.info("No numeric ML metrics available")
    else:
        st.info("No ML metrics available")

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =========================
# Details
# =========================
st.markdown(
    """
    <div class="section-title">Detail Tables</div>
    <div class="section-subtitle">Supporting data used by the dashboard</div>
    """,
    unsafe_allow_html=True
)

with st.expander("View KPI table"):
    if not kpi.empty:
        st.dataframe(kpi, use_container_width=True)

with st.expander("View channel table"):
    if not channel.empty:
        st.dataframe(channel, use_container_width=True)

with st.expander("View ML metrics table"):
    if not metrics.empty:
        st.dataframe(metrics, use_container_width=True)