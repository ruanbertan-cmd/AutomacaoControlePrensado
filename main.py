import pandas as pd
import os
from datetime import datetime
import re

# =============================================================================
# FUNÇÃO PARA LIMPAR CÓDIGOS
# =============================================================================
def limpar_codigo(df, colunas):
    """
    Remove '.0' e espaços extras dos códigos numéricos.
    """
    for col in colunas:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(".0", "", regex=False)
                .str.strip()
            )
    return df


# =============================================================================
# 1) IMPORTAÇÃO DAS BASES
# =============================================================================
df1 = pd.read_csv(r'X:\transf\LISTATOT_ep010r_ITENS.csv', sep=';', encoding='latin1', low_memory=False)
df2 = pd.read_csv(r'C:\Users\e145905\OneDrive - Mohawk Industries\Desktop\Pessoal\ruanbertan-cmd\revisaoItensPrensados\basesItens(Clone&Fundo)\ep270re14590555462.csv', sep=';', encoding='latin1', low_memory=False)
df3 = pd.read_csv(r'C:\Users\e145905\OneDrive - Mohawk Industries\Desktop\Pessoal\ruanbertan-cmd\revisaoItensPrensados\basesItens(Clone&Fundo)\ep235r55480.csv', sep=';', encoding='latin1', low_memory=False)

# =============================================================================
# 2) PADRONIZAÇÃO DE CÓDIGOS
# =============================================================================
df1 = limpar_codigo(df1, ["Codigo"])
df2 = limpar_codigo(df2, ["Item", "Item Fundo"])
df3 = limpar_codigo(df3, ["Item", "Item_Clone"])

# =============================================================================
# GARANTIR APENAS UM FUNDO POR ITEM (primeira ocorrência)
# =============================================================================
df2 = df2.sort_values(by=["Item"]).drop_duplicates(subset=["Item"], keep="first")

# =============================================================================
# 3) ADICIONAR INFORMAÇÕES DO FUNDO (para df2) e CLONES (para df3)
#    — trago situação e prensado do item fundo/clones para usar nas regras
# =============================================================================
df2 = df2.merge(
    df1[['Codigo', 'Prensado', 'Situacao', 'Unidade Pai', 'Polo']],
    left_on='Item Fundo',
    right_on='Codigo',
    how='left'
).drop(columns=['Codigo']).rename(columns={
    "Prensado": "PrensadoFundo",
    "Situacao": "SituacaoFundo",
    "Unidade Pai": "UnidadePaiFundo",
    "Polo": "PoloFundo"
})

df3 = df3.merge(
    df1[['Codigo', 'Prensado', 'Situacao', 'Polo']],
    left_on='Item_Clone',
    right_on='Codigo',
    how='left'
).drop(columns=['Codigo']).rename(columns={
    "Prensado": "PrensadoClone",
    "Situacao": "SituacaoClone",
    "Polo": "PoloClone"
})

# =============================================================================
# 4) MONTAR DF4 — base principal de trabalho
# =============================================================================
df4 = pd.DataFrame()
df4["CodigoOriginal"]        = df1["Codigo"].astype(str)
df4["DescOriginal"]          = df1["Produto"].astype(str)
df4["SituacaoOriginal"]      = df1["Situacao"].astype(str)
df4["UnOriginal"]            = df1["Un"].astype(str)
df4["TipologiaOriginal"]     = df1["Tipologia"].astype(str)
df4["PrensadoOriginal"]      = df1["Prensado"].astype(str)
df4["Dimensao"]              = df1["Dimensao"].astype(str)
df4["Nome Mae"]              = df1["Nome Mae"].astype(str)
df4["Acabamento Superficie"] = df1["Acabamento Superficie"].astype(str)
df4["Unidade Pai"]           = df1["Unidade Pai"].astype(str)
df4["Polo"]                  = df1["Polo"].astype(str)

# =============================================================================
# juntar dados de fundo (merge)
# =============================================================================
df4 = df4.merge(
    df2[['Item', 'Item Fundo', 'PrensadoFundo', 'SituacaoFundo', 'UnidadePaiFundo', 'PoloFundo']],
    left_on='CodigoOriginal',
    right_on='Item',
    how='left'
).drop(columns=['Item'])

# agrupar clones e juntar
df3_grouped = df3.groupby('Item')['Item_Clone'].apply(
    lambda x: list(x.dropna().astype(str).unique())
).reset_index()

df4 = df4.merge(
    df3_grouped,
    left_on='CodigoOriginal',
    right_on='Item',
    how='left'
).drop(columns=['Item'])

df4["ClonesLista"] = df4["Item_Clone"].apply(lambda x: x if isinstance(x, list) else [])
df4 = df4.drop(columns=["Item_Clone"])

# =============================================================================
# 5) CAMPOS DE CONTROLE — inicializar apenas UMA vez (antes de aplicar regras)
# =============================================================================
df4["PrensadoRevisado"] = None
df4["Validado"] = False

# =============================================================================
# AUXILIAR: recuperar índice (linha) pelo código
# =============================================================================
def get_row(code):
    result = df4.index[df4["CodigoOriginal"] == str(code)]
    return result[0] if len(result) else None

# =============================================================================
# AUXILIAR: validacao apenas por Polo

def filtrar_mesmo_polo(codigos, polo_original):
    """
    Retorna apenas códigos existentes no df4
    e que possuem o mesmo polo do item original.
    """
    if not codigos or not isinstance(codigos, (list, tuple)):
        return []

    filtrados = []
    for cod in codigos:
        idx = get_row(cod)
        if idx is None:
            continue
        if df4.at[idx, "Polo"] == polo_original:
            filtrados.append(cod)
    return filtrados

# =============================================================================

# =============================================================================
# FUNÇÃO PRINCIPAL: processar grupo (original + clones + opcional fundo)
#    — respeita itens já validados (não sobrescreve)
# =============================================================================
def processar_grupo(codigos):
    """
    Dado um grupo de códigos (original + clones + fundo),
    define quem será Prensado = Sim (menor código LB).
    Itens com Validado == True são ignorados.
    """
    if not codigos:
        return

    idx_ref = get_row(codigos[0])
    if idx_ref is None:
        return

    polo_ref = df4.at[idx_ref, "Polo"]
    codigos = filtrar_mesmo_polo(codigos, polo_ref)
    # limpar entradas inválidas
    codigos = [c for c in codigos if pd.notna(c) and str(c) != "nan"]

    # coletar só os pendentes (indices existentes e não validados)
    pendentes = []
    for cod in codigos:
        idx = get_row(cod)
        if idx is None:
            continue
        if df4.at[idx, "Validado"]:
            # item já travado por regra anterior -> ignora
            continue
        pendentes.append(cod)

    if not pendentes:
        return

    # agora dentre pendentes, pegar apenas os LB
    candidatos = []
    for cod in pendentes:
        idx = get_row(cod)
        if idx is None:
            continue
        if str(df4.at[idx, "SituacaoOriginal"]).strip().upper() == "LB":
            candidatos.append(cod)

    # se nenhum LB entre pendentes -> todos pendentes = NÃO
    if not candidatos:
        for cod in pendentes:
            idx = get_row(cod)
            if idx is not None and not df4.at[idx, "Validado"]:
                df4.at[idx, "PrensadoRevisado"] = "Nao"
                df4.at[idx, "Validado"] = True
        return

    # escolher menor código (numérico)
    menor = min(candidatos, key=lambda x: int(x))
    idx_menor = get_row(menor)

    # marcar vencedor SIM
    if idx_menor is not None and not df4.at[idx_menor, "Validado"]:
        df4.at[idx_menor, "PrensadoRevisado"] = "Sim"
        df4.at[idx_menor, "Validado"] = True

    # marcar demais pendentes como NÃO
    for cod in pendentes:
        if cod == menor:
            continue
        idx = get_row(cod)
        if idx is not None and not df4.at[idx, "Validado"]:
            df4.at[idx, "PrensadoRevisado"] = "Nao"
            df4.at[idx, "Validado"] = True

# =============================================================================
# 7) EXCEÇÃO 1 — FORMATOS 19,7×120 vs 60×120 (Somente SC3)
#    — APENAS para itens LB e que ainda não foram validados.
# =============================================================================

# Normalizar Unidade Pai e Dimensão para comparação robusta
df4["UnidadePaiNorm"] = df4["Unidade Pai"].astype(str).str.upper().str.replace(" ", "")
df4["DimensaoNorm"] = (
    df4["Dimensao"]
    .astype(str)
    .str.upper()
    .str.replace(" ", "")
    .str.replace("M", "")
)

# Identificadores dos formatos alvo
formato_menor = "19,7X120"
formato_maior = "60X120"

# Filtrar SC3 (qualquer string que contenha SC3)
mask_sc3 = df4["UnidadePaiNorm"].str.contains("SC3", na=False)

# Trabalhar apenas com linhas que são SC3 e ainda não validadas e LB
df_sc3 = df4[
    mask_sc3 &
    (df4["Validado"] == False) &
    (df4["SituacaoOriginal"].astype(str).str.upper().str.strip() == "LB")
].copy()

# Agrupar por Nome Mae
for nome, grupo in df_sc3.groupby("Nome Mae"):
    formatos = grupo["DimensaoNorm"].unique()

    # precisa conter ambos os formatos
    if formato_menor not in formatos or formato_maior not in formatos:
        continue

    # pegar índices no df4
    menor_rows = grupo[grupo["DimensaoNorm"] == formato_menor].index.tolist()
    maior_rows = grupo[grupo["DimensaoNorm"] == formato_maior].index.tolist()

    # se houver múltiplas ocorrências, iterar por pares - normalmente deve haver 1 de cada
    # vamos tratar cada combinação: para simplicidade pegar primeiro de cada
    if not menor_rows or not maior_rows:
        continue

    idx_menor = menor_rows[0]
    idx_maior = maior_rows[0]

    # garantir que ambos sejam LB (requisito)
    if (str(df4.at[idx_menor, "SituacaoOriginal"]).strip().upper() != "LB" or
            str(df4.at[idx_maior, "SituacaoOriginal"]).strip().upper() != "LB"):
        continue

    # --- bloquear o menor como NÃO (inclui fundo e clones) ---
    if not df4.at[idx_menor, "Validado"]:
        df4.at[idx_menor, "PrensadoRevisado"] = "Nao"
        df4.at[idx_menor, "Validado"] = True

    # fundo do menor
    polo_ref = df4.at[idx_menor, "Polo"]
    fundo_menor = df4.at[idx_menor, "Item Fundo"]

    if pd.notna(fundo_menor):
        f = filtrar_mesmo_polo([fundo_menor], polo_ref)
        if f:
            fidx = get_row(f[0])
            if fidx is not None and not df4.at[fidx, "Validado"]:
                df4.at[fidx, "PrensadoRevisado"] = "Nao"
                df4.at[fidx, "Validado"] = True

    # clones do menor
    for clone in df4.at[idx_menor, "ClonesLista"]:
        cidx = get_row(clone)
        if cidx is not None and not df4.at[cidx, "Validado"]:
            df4.at[cidx, "PrensadoRevisado"] = "Nao"
            df4.at[cidx, "Validado"] = True

    # --- preparar e processar apenas o grupo relativo ao 60x120 ---
    cod_maior = df4.at[idx_maior, "CodigoOriginal"]
    polo_ref = df4.at[idx_maior, "Polo"]
    clones_validos = filtrar_mesmo_polo(df4.at[idx_maior, "ClonesLista"], polo_ref)
    grupo_maior = [cod_maior] + clones_validos

    fundo_maior = df4.at[idx_maior, "Item Fundo"]
    # incluir o fundo do maior apenas se existir (processar_grupo ignora já validados)
    if pd.notna(fundo_maior):
        f = filtrar_mesmo_polo([fundo_maior], polo_ref)
        if f:
            grupo_maior.append(f[0])

    # processar grupo (processar_grupo respeita Validado==True)
    processar_grupo(grupo_maior)

# =============================================================================
# 8) EXCEÇÃO 2 — ACABAMENTO NATURAL x POLIDO (somente SC4)
#    Grupo = Nome Mae + Dimensão
#    Se houver NATURAL + POLIDO → NATURAL = Sim / POLIDO = Não
# =============================================================================

# Normalizações
df4["UnidadePaiNorm"] = df4["Unidade Pai"].astype(str).str.upper().str.replace(" ", "")
df4["DimensaoNorm"] = (
    df4["Dimensao"]
    .astype(str)
    .str.upper()
    .str.replace(" ", "")
    .str.replace(",", ".")
)
df4["AcabamentoNorm"] = (
    df4["Acabamento Superficie"]
    .astype(str)
    .str.upper()
    .str.replace(" ", "")
)

# Trabalhar apenas com SC4, LB e ainda não validados
df_sc4 = df4[
    (df4["Validado"] == False) &
    (df4["SituacaoOriginal"].str.upper().str.strip() == "LB") &
    (df4["UnidadePaiNorm"].str.contains("SC4", na=False))
].copy()

# garantir separação por polo
df_sc4["Polo"] = df_sc4["Polo"].astype(str)


# Revisando campo Nome Mae para nao ter acabamento
df_sc4["Nome Mae"] = (
    df_sc4["Nome Mae"]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(
            r"\s+(NA|PO|POL|NAT)$",
            "",
            regex=True
        )
)

# Agrupar por Nome Mae + Dimensão
for (polo, nome_mae, dimensao), grupo in df_sc4.groupby(
    ["Polo", "Nome Mae", "DimensaoNorm"]
):


    acabamentos = set(grupo["AcabamentoNorm"].unique())

    # Precisa existir NATURAL e POLIDO
    if not {"NATURAL", "POLIDO"}.issubset(acabamentos):
        continue

    # Separar índices
    idx_nat = grupo[grupo["AcabamentoNorm"] == "NATURAL"].index.tolist()
    idx_pol = grupo[grupo["AcabamentoNorm"] == "POLIDO"].index.tolist()

    if not idx_nat or not idx_pol:
        continue

    # ============================
    # NATURAL = SIM
    # ============================
    for idx in idx_nat:
        if df4.at[idx, "Validado"]:
            continue
        df4.at[idx, "PrensadoRevisado"] = "Sim"
        df4.at[idx, "Validado"] = True

    # ============================
    # POLIDO = NÃO
    # ============================
    for idx in idx_pol:
        if df4.at[idx, "Validado"]:
            continue
        df4.at[idx, "PrensadoRevisado"] = "Nao"
        df4.at[idx, "Validado"] = True


# =============================================================================
# 6) REGRAS INICIAIS (aplicadas com proteção: não sobrescrevem itens já validados)
# =============================================================================

# Situação diferente de LB -> NÃO (trava e valida)
mask_not_lb = (
    (df4["Validado"] == False) &
    (df4["SituacaoOriginal"].astype(str).str.upper().str.strip() != "LB")
)
df4.loc[mask_not_lb, ["PrensadoRevisado", "Validado"]] = ["Nao", True]

# Unidade diferente de m2
mask_un = (
    (df4["Validado"] == False) &
    (~df4["UnOriginal"].astype(str).str.lower().isin(["m2"]))
)
df4.loc[mask_un, ["PrensadoRevisado", "Validado"]] = ["Nao", True]

# Tipologia inválida -> NÃO
tipos_validos = ["PORC GL", "PORC UGL", "MONOPOROSA", "MONOQUEIMA", "BIQUEIMA", "GRES"]
mask_tip = (
    (df4["Validado"] == False) &
    (~df4["TipologiaOriginal"].astype(str).str.upper().isin(tipos_validos))
)

df4.loc[mask_tip, ["PrensadoRevisado", "Validado"]] = ["Nao", True]

# Unidade Pai inválida -> NÃO
uniPai_validos = ["SC 1", "SC 2", "SC 3", "SC 4", "SC 5", "ART. I - SC", "BA 1", "PB 1"]
mask_pai = (
    (df4["Validado"] == False) &
    (~df4["Unidade Pai"].astype(str).str.upper().isin(uniPai_validos))
)

df4.loc[mask_pai, ["PrensadoRevisado", "Validado"]] = ["Nao", True]

# Regra global específica: itens em AMOSTRA NÃO podem ser prensados (ex.: FP -> Nao)
mask_desc = (
    (df4["Validado"] == False) &
    (
        df4["DescOriginal"]
        .astype(str)
        .str.upper()
        .str.contains(
            r"\b(?:AMOSTRA|SAMPLE|BOARD|PLACA|EXPOSITOR)\b",
            regex=True
        )
    )
)

df4.loc[mask_desc, ["PrensadoRevisado", "Validado"]] = ["Nao", True]

# Nota: todas as regras acima definem Validado=True para bloquear reescrita posterior.


# =============================================================================
# 9) REGRA FINAL (TRIPLA CHECAGEM)
# =============================================================================
for idx, row in df4[
    (df4["Validado"] == False) &
    (df4["SituacaoOriginal"].astype(str).str.upper().str.strip() == "LB")
].iterrows():

    original = row["CodigoOriginal"]
    polo_original = df4.at[idx, "Polo"]

    clones = filtrar_mesmo_polo(row["ClonesLista"], polo_original)

    fundo = None
    if pd.notna(row["Item Fundo"]):
        fundo_f = filtrar_mesmo_polo([row["Item Fundo"]], polo_original)
        fundo = fundo_f[0] if fundo_f else None

    tipo_fundo = row["SituacaoFundo"]

    # Caso A — TEM FUNDO E FUNDO É LB
    if fundo and str(tipo_fundo).strip().upper() == "LB":

        fund_idx = get_row(fundo)
        orig_idx = get_row(original)

        if fund_idx is not None and not df4.at[fund_idx, "Validado"]:
            df4.at[fund_idx, "PrensadoRevisado"] = "Sim"
            df4.at[fund_idx, "Validado"] = True

        if orig_idx is not None and not df4.at[orig_idx, "Validado"]:
            df4.at[orig_idx, "PrensadoRevisado"] = "Nao"
            df4.at[orig_idx, "Validado"] = True

        for clone in clones:
            cidx = get_row(clone)
            if cidx is not None and not df4.at[cidx, "Validado"]:
                df4.at[cidx, "PrensadoRevisado"] = "Nao"
                df4.at[cidx, "Validado"] = True
        continue

    # Caso B — fundo existe e não é LB
    if fundo:
        grupo = [original] + clones + [fundo]
        processar_grupo(grupo)
        continue

    # Caso C — sem fundo
    processar_grupo([original] + clones)


# =============================================================================
# 10) MONTAR DF5 — base principal de trabalho
# =============================================================================
df5 = pd.DataFrame()
df5["Unidade Pai"]           = df4["Unidade Pai"].astype(str)
df5["CodigoOriginal"]        = df4["CodigoOriginal"].astype(str)
df5["DescricaoOriginal"]     = df4["DescOriginal"].astype(str)
df5["SituacaoOriginal"]      = df4["SituacaoOriginal"].astype(str)
df5["UnOriginal"]            = df4["UnOriginal"].astype(str)
df5["TipologiaOriginal"]     = df4["TipologiaOriginal"].astype(str)
df5["Item Fundo"]            = df4["Item Fundo"].astype(str)
df5["SituacaoFundo"]         = df4["SituacaoFundo"].astype(str)
df5["ClonesLista"]           = df4["ClonesLista"].astype(str)
df5["PrensadoRevisado"]      = df4["PrensadoRevisado"].astype(str)



# =============================================================================
# 10) SALVAR ARQUIVO geral usado para desenvolvimento e ver situacao Macro
# =============================================================================
df4.to_csv("analisePrensado_Final.csv", sep=";", index=False)
print("✔ Arquivo completo gerado com sucesso: AnalisePrensado_Final.csv")

# =============================================================================
# 11) SALVAR ARQUIVO modelo Log de Revisão
# =============================================================================

# pasta de destino
pasta = r"log"

# garante que a pasta exista
os.makedirs(pasta, exist_ok=True)

# data no formato desejado
data_hoje = datetime.now().strftime("%d-%m-%Y")

# nome do arquivo
nome_arquivo = f"analisePrensado_{data_hoje}.csv"

# caminho completo
caminho_arquivo = os.path.join(pasta, nome_arquivo)

# salvar
df5.to_csv(caminho_arquivo, sep=";", index=False)

print(f"✔ Arquivo completo gerado com sucesso: {caminho_arquivo}")
