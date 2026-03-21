import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

st.set_page_config(
    page_title="Dashboard de Analítica de Fraude",
    layout="wide",
    initial_sidebar_state="collapsed"
)

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

kpi = pd.read_sql("select * from public.mart_fraud_kpi limit 1", engine)
channel = pd.read_sql("select * from public.mart_fraud_by_channel", engine)
metrics = pd.read_sql("select * from public.ml_model_metrics", engine)

def tarjeta_metrica(titulo: str, valor: str):
    st.markdown(
        f"""
        <div class="dashboard-card">
            <div class="metric-accent"></div>
            <div class="metric-label">{titulo}</div>
            <div class="metric-value">{valor}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown(
    """
    <div class="dashboard-card">
        <div class="section-title" style="font-size: 2rem; margin-bottom: 0.3rem;">
            Dashboard de Analítica de Fraude
        </div>
        <div class="section-subtitle">
            Prototipo para monitoreo de fraude, analítica dimensional y evaluación de modelo de machine learning
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class="section-title">Resumen Ejecutivo</div>
    <div class="section-subtitle">Indicadores principales generados desde el mart analítico</div>
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
        tarjeta_metrica("Transacciones", f"{tx_count:,}")
    with c2:
        tarjeta_metrica("Transacciones con fraude", f"{fraud_tx_count:,}")
    with c3:
        tarjeta_metrica("Tasa de fraude", f"{fraud_rate:.4f}")
    with c4:
        tarjeta_metrica("Monto total", f"{amount_total:,.2f}")
else:
    st.warning("No hay datos disponibles en public.mart_fraud_kpi")

st.markdown("<hr>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="section-title">Monitoreo de Fraude por Canal</div>
    <div class="section-subtitle">Vista comparativa del volumen de fraude y la tasa de fraude</div>
    """,
    unsafe_allow_html=True
)

left, right = st.columns(2)

with left:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown("**Transacciones fraudulentas por canal**")

    if not channel.empty and "channel" in channel.columns and "fraud_tx_count" in channel.columns:
        fig, ax = plt.subplots(figsize=(6, 3.8))
        ax.bar(channel["channel"], channel["fraud_tx_count"])
        ax.set_title("Volumen de fraude", pad=12)
        ax.set_xlabel("Canal")
        ax.set_ylabel("Transacciones con fraude")
        ax.grid(axis="y", alpha=0.25)
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)
    else:
        st.info("No hay datos de fraude por canal disponibles")
    st.markdown('</div>', unsafe_allow_html=True)

with right:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
    st.markdown("**Tasa de fraude por canal**")

    if not channel.empty and "channel" in channel.columns and "fraud_rate" in channel.columns:
        fig2, ax2 = plt.subplots(figsize=(6, 3.8))
        ax2.bar(channel["channel"], channel["fraud_rate"])
        ax2.set_title("Tasa de fraude", pad=12)
        ax2.set_xlabel("Canal")
        ax2.set_ylabel("Tasa")
        ax2.grid(axis="y", alpha=0.25)
        st.pyplot(fig2, use_container_width=True)
        plt.close(fig2)
    else:
        st.info("No hay datos de tasa de fraude por canal disponibles")
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="section-title">Modelo de Machine Learning</div>
    <div class="section-subtitle">Métricas persistidas del proceso de entrenamiento ejecutado por ml/train_model.py</div>
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
            **Evidencia del modelo**
            - Script de entrenamiento: `ml/train_model.py`
            - Métricas persistidas: `public.ml_model_metrics`
            - Artefacto local generado: `artifacts/model.joblib`
            """
        )
    else:
        st.warning("No hay métricas del modelo disponibles")

    st.markdown('</div>', unsafe_allow_html=True)

with ml_right:
    st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)

    if not metrics.empty:
        numeric_cols = metrics.select_dtypes(include=["number"]).columns.tolist()

        if numeric_cols:
            fig3, ax3 = plt.subplots(figsize=(7, 3.8))
            ax3.bar(numeric_cols, [float(metrics[c].iloc[0]) for c in numeric_cols])
            ax3.set_title("Resumen de desempeño del modelo", pad=12)
            ax3.set_ylabel("Valor de la métrica")
            ax3.grid(axis="y", alpha=0.25)
            plt.xticks(rotation=25)
            st.pyplot(fig3, use_container_width=True)
            plt.close(fig3)
        else:
            st.info("No hay métricas numéricas disponibles")
    else:
        st.info("No hay métricas del modelo disponibles")

    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="section-title">Tablas de detalle</div>
    <div class="section-subtitle">Información de soporte utilizada por el dashboard</div>
    """,
    unsafe_allow_html=True
)

with st.expander("Ver tabla KPI"):
    if not kpi.empty:
        st.dataframe(kpi, use_container_width=True)

with st.expander("Ver tabla por canal"):
    if not channel.empty:
        st.dataframe(channel, use_container_width=True)

with st.expander("Ver tabla de métricas del modelo"):
    if not metrics.empty:
        st.dataframe(metrics, use_container_width=True)