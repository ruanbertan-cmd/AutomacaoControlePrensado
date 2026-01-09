import os
import subprocess
import pandas as pd
import streamlit as st

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_OUTPUT = os.path.join(BASE_DIR, "output")

ARQ_KPI = os.path.join(PASTA_OUTPUT, "kpi_lb_prensado.csv")
IMG_EVOLUCAO = os.path.join(PASTA_OUTPUT, "evolucao_pct_lb_prensado.png")
IMG_RANKING = os.path.join(PASTA_OUTPUT, "ranking_unidade_lb_prensado.png")

SCRIPT_GERACAO = os.path.join(BASE_DIR, "modeloVisualMovimentacoes.py")

# =============================================================================
# STREAMLIT CONFIG
# =============================================================================
st.set_page_config(page_title="Dashboard Prensados", layout="wide", page_icon="📊")
st.title("📊 Dashboard de Prensados — Itens LB")

# =============================================================================
# BOTÃO ATUALIZAR
# =============================================================================
with st.sidebar:
    if st.button("🔄 Atualizar dados"):
        with st.spinner("Atualizando dados..."):
            subprocess.run(["python", SCRIPT_GERACAO], shell=True)
        st.success("Dados atualizados com sucesso!")
        st.rerun()

# =============================================================================
# VALIDAÇÃO
# =============================================================================
if not os.path.exists(ARQ_KPI):
    st.error("❌ KPI não encontrado.")
    st.info("Clique em **Atualizar dados** para gerar.")
    st.stop()

kpi = pd.read_csv(ARQ_KPI, sep=";").iloc[0]

# =============================================================================
# KPIs — CARDS
# =============================================================================
c1, c2, c3 = st.columns(3)

c1.metric(
    "% LB Prensado (Atual)",
    f"{kpi['pct_lb_prensado_atual']}%",
    f"{kpi['qtd_lb_prensado_atual']} / {kpi['total_lb_atual']}"
)

c2.metric(
    "% LB Prensado (Anterior)",
    f"{kpi['pct_lb_prensado_anterior']}%",
    f"{kpi['qtd_lb_prensado_anterior']} / {kpi['total_lb_anterior']}"
)

c3.metric(
    "Variação",
    f"{kpi['variacao_pp']} p.p.",
    delta=f"{kpi['variacao_pp']} p.p."
)

st.divider()

# =============================================================================
# GRÁFICOS
# =============================================================================
g1, g2 = st.columns(2)

with g1:
    st.subheader("📈 Evolução % e Quantidade de LB Prensado")
    if os.path.exists(IMG_EVOLUCAO):
        st.image(IMG_EVOLUCAO, width="stretch")

with g2:
    st.subheader("🏭 Ranking por Unidade Pai")
    if os.path.exists(IMG_RANKING):
        st.image(IMG_RANKING, width="stretch")

st.caption("📌 Percentual acompanhado da quantidade absoluta de itens LB prensados.")
st.caption("Desenvolvido por Ruan Bertan.")