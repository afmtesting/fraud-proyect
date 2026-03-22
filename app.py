import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt

from sklearn.metrics import (
    confusion_matrix,
    precision_score,
    recall_score,
    f1_score,
    accuracy_score,
    precision_recall_curve,
    average_precision_score
)

load_dotenv()

st.set_page_config(
    page_title="Dashboard de Analítica de Fraude",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# =========================
# Estilos
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

# =========================
# Conexión
# =========================
pg_url = os.getenv("PG_URL")
if not pg_url:
    raise ValueError("No se encontró PG_URL en el entorno.")

engine = create_engine(pg_url)

# =========================
# Helpers
# =========================
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

@st.cache_data(ttl=300)
def cargar_datos():
    kpi = pd.read_sql("select * from public.mart_fraud_kpi limit 1", engine)
    channel = pd.read_sql("select * from public.mart_fraud_by_channel", engine)
    metrics = pd.read_sql("select * from public.ml_model_metrics", engine)
    eval_preds = pd.read_sql("select * from public.ml_eval_predictions", engine)
    fi = pd.read_sql("select * from public.ml_feature_importance order by importance desc limit 10", engine)
    return kpi, channel, metrics, eval_preds, fi

kpi, channel, metrics, eval_preds, fi = cargar_datos()

# =========================
# Header
# =========================
st.markdown(
    """
    <div class="dashboard-card">
        <div class="section-title" style="font-size: 2rem; margin-bottom: 0.3rem;">
            Dashboard de Analítica de Fraude
        </div>
        <div class="section-subtitle">
            Prototipo para monitoreo de fraude, analítica dimensional y evaluación del modelo predictivo
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# =========================
# KPIs generales
# =========================
st.markdown(
    """
    <div class="section-title">Resumen Ejecutivo</div>
    <div class="section-subtitle">Indicadores generales del mart analítico</div>
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

# =========================
# Análisis por canal
# =========================
st.markdown(
    """
    <div class="section-title">Monitoreo de Fraude por Canal</div>
    <div class="section-subtitle">Vista comparativa del volumen y la tasa de fraude</div>
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

# =========================
# Modelo ML
# =========================
st.markdown(
    """
    <div class="section-title">Modelo de Machine Learning</div>
    <div class="section-subtitle">Evaluación visual y métricas del Random Forest sin reentrenamiento</div>
    """,
    unsafe_allow_html=True
)

if eval_preds.empty:
    st.warning("No existe información en public.ml_eval_predictions. Ejecuta una vez ml/train_model.py.")
else:
    threshold = st.slider(
        "Umbral de clasificación",
        min_value=0.10,
        max_value=0.90,
        value=0.50,
        step=0.05
    )

    y_true = eval_preds["y_true"].astype(int)
    y_score = eval_preds["y_score"].astype(float)
    y_pred = (y_score >= threshold).astype(int)

    # Métricas dinámicas
    accuracy_dyn = accuracy_score(y_true, y_pred)
    precision_dyn = precision_score(y_true, y_pred, zero_division=0)
    recall_dyn = recall_score(y_true, y_pred, zero_division=0)
    f1_dyn = f1_score(y_true, y_pred, zero_division=0)
    pr_auc = average_precision_score(y_true, y_score)

    st.markdown(
        """
        <div class="section-subtitle">Métricas del modelo para el umbral seleccionado</div>
        """,
        unsafe_allow_html=True
    )

    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        tarjeta_metrica("Accuracy", f"{accuracy_dyn:.4f}")
    with m2:
        tarjeta_metrica("Precision", f"{precision_dyn:.4f}")
    with m3:
        tarjeta_metrica("Recall", f"{recall_dyn:.4f}")
    with m4:
        tarjeta_metrica("F1-Score", f"{f1_dyn:.4f}")
    with m5:
        tarjeta_metrica("PR AUC", f"{pr_auc:.4f}")

    st.markdown("<hr>", unsafe_allow_html=True)

    ml_left, ml_right = st.columns(2)

    with ml_left:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown("**Matriz de confusión**")

        cm = confusion_matrix(y_true, y_pred)
        tn, fp, fn, tp = cm.ravel()

        fig_cm, ax_cm = plt.subplots(figsize=(6, 5))
        im = ax_cm.imshow(cm)

        ax_cm.set_xticks([0, 1])
        ax_cm.set_yticks([0, 1])
        ax_cm.set_xticklabels(["Predicción: No fraude", "Predicción: Fraude"])
        ax_cm.set_yticklabels(["Real: No fraude", "Real: Fraude"])
        ax_cm.set_title("Matriz de confusión", pad=12)

        labels = [
            [f"Verdaderos Negativos\n(TN)\n{tn}", f"Falsos Positivos\n(FP)\n{fp}"],
            [f"Falsos Negativos\n(FN)\n{fn}", f"Verdaderos Positivos\n(TP)\n{tp}"]
        ]

        for i in range(2):
            for j in range(2):
                ax_cm.text(j, i, labels[i][j], ha="center", va="center", fontsize=11)

        fig_cm.colorbar(im, ax=ax_cm, fraction=0.046, pad=0.04)
        st.pyplot(fig_cm, use_container_width=True)
        plt.close(fig_cm)

        st.markdown('</div>', unsafe_allow_html=True)

    with ml_right:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown("**Curva Precision-Recall**")

        precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_score)

        fig_pr, ax_pr = plt.subplots(figsize=(6, 5))
        ax_pr.plot(recall_curve, precision_curve)
        ax_pr.fill_between(recall_curve, precision_curve, alpha=0.2)
        ax_pr.set_title(f"Curva Precision-Recall (AUC = {pr_auc:.4f})", pad=12)
        ax_pr.set_xlabel("Recall")
        ax_pr.set_ylabel("Precision")
        ax_pr.grid(alpha=0.25)
        st.pyplot(fig_pr, use_container_width=True)
        plt.close(fig_pr)

        st.markdown(
            """
            **Interpretación**
            - La curva muestra el equilibrio entre precisión y sensibilidad.
            - Para problemas desbalanceados, esta visual es más informativa que accuracy.
            """
        )
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    bottom_left, bottom_right = st.columns([1.2, 1.8])

    with bottom_left:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown("**Evidencia del modelo**")
        st.markdown(
            """
            - Script de entrenamiento: `ml/train_model.py`
            - Métricas persistidas: `public.ml_model_metrics`
            - Predicciones de evaluación: `public.ml_eval_predictions`
            - El modelo no se reentrena en el dashboard
            """
        )
        st.markdown('</div>', unsafe_allow_html=True)

    with bottom_right:
        st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
        st.markdown("**Variables más importantes**")
        if not fi.empty:
            fig_fi, ax_fi = plt.subplots(figsize=(8, 4))
            ax_fi.barh(fi["feature"][::-1], fi["importance"][::-1])
            ax_fi.set_title("Importancia de variables", pad=12)
            ax_fi.set_xlabel("Importancia")
            ax_fi.grid(axis="x", alpha=0.25)
            st.pyplot(fig_fi, use_container_width=True)
            plt.close(fig_fi)
        else:
            st.info("No hay datos de importancia de variables.")
        st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<hr>", unsafe_allow_html=True)

# =========================
# Tablas de detalle
# =========================
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

with st.expander("Ver predicciones de evaluación"):
    if not eval_preds.empty:
        st.dataframe(eval_preds.head(200), use_container_width=True)