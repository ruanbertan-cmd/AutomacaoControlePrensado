import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
PASTA_LOG = "log"
PASTA_OUTPUT = "output"
PREFIXO_ARQUIVO = "analisePrensado_"
SEPARADOR = ";"
TOP_N_UNIDADES = 10
MOSTRAR_GRAFICOS: bool = True

os.makedirs(PASTA_OUTPUT, exist_ok=True)

# =============================================================================
# FUNÇÃO — extrair data do nome do arquivo
# =============================================================================
def extrair_data(nome):
    return nome.replace(PREFIXO_ARQUIVO, "").replace(".csv", "")

# =============================================================================
# 1) CARREGAR HISTÓRICO
# =============================================================================
arquivos = sorted([
    f for f in os.listdir(PASTA_LOG)
    if f.startswith(PREFIXO_ARQUIVO) and f.endswith(".csv")
])

if not arquivos:
    raise ValueError("Nenhum arquivo de histórico encontrado.")

dados = []
for arq in arquivos:
    df = pd.read_csv(os.path.join(PASTA_LOG, arq), sep=SEPARADOR)
    df["Data"] = extrair_data(arq)
    dados.append(df)

df_hist = pd.concat(dados, ignore_index=True)
df_hist["Data_dt"] = pd.to_datetime(df_hist["Data"], dayfirst=True, errors="coerce")

# =============================================================================
# 2) KPI — LB PRENSADO (% + QUANTIDADE)
# =============================================================================
lb_hist = df_hist[
    df_hist["SituacaoOriginal"].astype(str).str.upper().str.strip() == "LB"
].copy()

datas = sorted(lb_hist["Data_dt"].dropna().unique())

data_atual = data_anterior = None
kpi = {}

if len(datas) >= 2:
    data_atual = datas[-1]
    data_anterior = datas[-2]

    lb_atual = lb_hist[lb_hist["Data_dt"] == data_atual]
    lb_ant   = lb_hist[lb_hist["Data_dt"] == data_anterior]

    qtd_lb_atual = len(lb_atual)
    qtd_sim_atual = (lb_atual["PrensadoRevisado"] == "Sim").sum()

    qtd_lb_ant = len(lb_ant)
    qtd_sim_ant = (lb_ant["PrensadoRevisado"] == "Sim").sum()

    pct_atual = (qtd_sim_atual / qtd_lb_atual) * 100 if qtd_lb_atual else 0
    pct_ant   = (qtd_sim_ant / qtd_lb_ant) * 100 if qtd_lb_ant else 0

    delta = pct_atual - pct_ant

    kpi = {
        "data_atual": data_atual.strftime("%Y-%m-%d"),
        "pct_lb_prensado_atual": round(pct_atual, 2),
        "qtd_lb_prensado_atual": int(qtd_sim_atual),
        "total_lb_atual": int(qtd_lb_atual),

        "pct_lb_prensado_anterior": round(pct_ant, 2),
        "qtd_lb_prensado_anterior": int(qtd_sim_ant),
        "total_lb_anterior": int(qtd_lb_ant),

        "variacao_pp": round(delta, 2)
    }

    print("\n📌 KPI — LB PRENSADO")
    print(kpi)

else:
    print("\n⚠️ Histórico insuficiente para KPI.")

# =============================================================================
# 3) SALVAR KPI
# =============================================================================
if kpi:
    pd.DataFrame([kpi]).to_csv(
        os.path.join(PASTA_OUTPUT, "kpi_lb_prensado.csv"),
        index=False,
        sep=";"
    )

# =============================================================================
# 4) GRÁFICO — % + QTD LB PRENSADO AO LONGO DO TEMPO
# =============================================================================
resumo_pct = (
    lb_hist
    .groupby("Data", as_index=False)
    .agg(
        Total_LB=("CodigoOriginal", "count"),
        Qtd_Prensado_Sim=("PrensadoRevisado", lambda x: (x == "Sim").sum())
    )
)

resumo_pct["Pct_LB_Prensado"] = (
    resumo_pct["Qtd_Prensado_Sim"] / resumo_pct["Total_LB"] * 100
)

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(
    resumo_pct["Data"],
    resumo_pct["Pct_LB_Prensado"],
    marker="o",
    linewidth=2.5,
    color="#1565C0"
)

for _, row in resumo_pct.iterrows():
    ax.text(
        row["Data"],
        row["Pct_LB_Prensado"],
        f'{row["Pct_LB_Prensado"]:.1f}% ({row["Qtd_Prensado_Sim"]})',
        ha="center",
        va="bottom",
        fontsize=9
    )

ax.set_title("Percentual e Quantidade de Itens LB Prensados",
             fontsize=14, fontweight="bold", loc="left")
ax.set_ylabel("% de LB Prensado")
ax.set_xlabel("Data")
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig(
    os.path.join(PASTA_OUTPUT, "ranking_unidade_lb_prensado.png"),
    dpi=300,
    bbox_inches="tight"
)

if MOSTRAR_GRAFICOS:
    plt.show()

plt.close()

# =============================================================================
# 5) RANKING — % + QTD POR UNIDADE PAI
# =============================================================================
if data_atual is not None:
    lb_atual = lb_hist[lb_hist["Data_dt"] == data_atual]

    resumo_unidade = (
        lb_atual
        .groupby("Unidade Pai", as_index=False)
        .agg(
            Total_LB=("CodigoOriginal", "count"),
            Qtd_Prensado_Sim=("PrensadoRevisado", lambda x: (x == "Sim").sum())
        )
    )

    resumo_unidade["Pct_LB_Prensado"] = (
        resumo_unidade["Qtd_Prensado_Sim"] / resumo_unidade["Total_LB"] * 100
    )

    resumo_unidade = (
        resumo_unidade
        .sort_values("Pct_LB_Prensado", ascending=False)
        .head(TOP_N_UNIDADES)
    )

    fig, ax = plt.subplots(figsize=(5, 5))
    ax.barh(
        resumo_unidade["Unidade Pai"],
        resumo_unidade["Pct_LB_Prensado"],
        color="#2E7D32"
    )

    for i, row in resumo_unidade.iterrows():
        pct = row["Pct_LB_Prensado"]
        qtd = int(row["Qtd_Prensado_Sim"])

        # Se estiver muito à direita, escreve dentro da barra
        if pct >= 90:
            ax.text(
                pct - 2,
                i,
                f"{pct:.1f}% ({qtd})",
                va="center",
                ha="right",
                color="white",
                fontweight="bold"
            )
        else:
            ax.text(
                pct + 1,
                i,
                f"{pct:.1f}% ({qtd})",
                va="center",
                ha="left",
                color="black"
            )

    ax.set_xlim(0, 105)
    ax.set_title(f"% e Quantidade de LB Prensado por Unidade ({data_atual.date()})",
                 fontsize=14, fontweight="bold", loc="left")
    ax.set_xlabel("% de LB Prensado")
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(
    os.path.join(PASTA_OUTPUT, "ranking_unidade_lb_prensado.png"),
    dpi=300,
    bbox_inches="tight"
)
    if MOSTRAR_GRAFICOS:
        plt.show()
    plt.close()
