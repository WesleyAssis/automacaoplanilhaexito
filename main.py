import pdfplumber
import pandas as pd
import requests
import re
import time
from datetime import datetime
from functools import reduce
from operator import mul
from io import BytesIO
import os

import streamlit as st

# =====================================================
# MAPA DE MESES + VARIACOES DE COLUNAS
# =====================================================
MESES = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
    "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12"
}

COLUMN_VARIANTS = {
    "Mês referência/Ano cobrança": ["Referência", "Dta.Ref"],
    "Pagamento": ["Pagamento", "Data.Pagto"],
    "Pontos": ["Pontos", "Ptos./Qtd."],
    "Preço Pto.": ["Preço Pto.", "Prç.Pto./Vlr."],
    "Vencimento": ["Vencimento", "Data Vencimento", "Data.Venc."]
}

# =====================================================
# UTILITÁRIOS
# =====================================================
def yyyymm_para_data_bacen(yyyymm):
    if not yyyymm or len(yyyymm) != 6 or not yyyymm.isdigit():
        return None
    ano = yyyymm[:4]
    mes = yyyymm[4:]
    return f"01/{mes}/{ano}"

def converter_referencia_yyyymm(valor):
    try:
        valor = str(valor).strip().lower()
        mes, ano = re.split(r"[/\s]+", valor)
        return f"{ano}{MESES[mes[:3]]}"
    except Exception:
        return ""

def converter_referencia_yyyymm_pagamento(valor):
    try:
        if pd.isna(valor): return ""
        data = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        return data.strftime("%Y%m") if pd.notna(data) else ""
    except Exception:
        return ""

def eh_mes_ano(valor):
    if pd.isna(valor): return False
    return bool(re.match(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)/\d{4}$", str(valor).lower().strip()))

def tratar_postes(valor):
    if pd.isna(valor): return 0
    texto = str(valor).replace(" ", "").replace(".", "").replace(",", "")
    return int(texto) if texto.isdigit() else 0

def tratar_numero(valor):
    if pd.isna(valor): return 0.0
    texto = str(valor).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try: return float(texto)
    except: return 0.0

def add_meses_yyyymm(yyyymm, meses):
    if not yyyymm or len(yyyymm) != 6: return ""
    y = int(yyyymm[:4])
    m = int(yyyymm[4:])
    new_m = m + meses
    add_y = (new_m - 1) // 12
    new_m = (new_m - 1) % 12 + 1
    return f"{y + add_y:04d}{new_m:02d}"

def calcular_meses_diff(yyyymm1, yyyymm2):
    if not yyyymm1 or not yyyymm2: return 0
    y1, m1 = int(yyyymm1[:4]), int(yyyymm1[4:])
    y2, m2 = int(yyyymm2[:4]), int(yyyymm2[4:])
    return (y2 - y1) * 12 + (m2 - m1)

def carregar_icgj():
    try:
        base_dir = os.path.dirname(__file__)
        caminho = os.path.join(base_dir, "ICGJ.csv")
        df = pd.read_csv(caminho)
        df["Referencia_yyyymm"] = df["Referencia_yyyymm"].astype(str).str.strip()
        df["Indice"] = df["Indice"].astype(str).str.replace(",", ".").astype(float)
        return df[["Referencia_yyyymm", "Indice"]]
    except Exception as e:
        st.error(f"Erro ao carregar ICGJ.csv: {e}")
        return pd.DataFrame(columns=["Referencia_yyyymm", "Indice"])

def recalcular_icgj(df):
    """
    Calcula o ICGJ linha a linha usando dicionário.
    Prioridade: referencia_pgto → referencia → 1.0 (neutro, sem correção)
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    icgj_df = carregar_icgj()
    if icgj_df.empty:
        df["ICGJ"] = 1.0
        return df

    # Dicionário rápido: Referencia_yyyymm → Indice
    icgj_dict = dict(zip(icgj_df["Referencia_yyyymm"], icgj_df["Indice"]))

    def obter_icgj(row):
        # Prioridade 1: referencia_pgto
        ref_pgto = row.get("referencia_pgto")
        if pd.notna(ref_pgto):
            ref_pgto_str = str(ref_pgto).strip()
            if ref_pgto_str and ref_pgto_str != "" and ref_pgto_str in icgj_dict:
                return icgj_dict[ref_pgto_str]

        # Prioridade 2: referencia
        ref = row.get("referencia")
        if pd.notna(ref):
            ref_str = str(ref).strip()
            if ref_str and ref_str != "" and ref_str in icgj_dict:
                return icgj_dict[ref_str]

        # Nenhum encontrado → retorna 1.0 (sem correção)
        return 1.0

    df["ICGJ"] = df.apply(obter_icgj, axis=1)
    return df

def consultar_bacen_fator(codigo, data_ref, data_final=None, tentativas=3, delay=3, considerar_negativo=False):
    if data_final is None:
        data_final = datetime.today().strftime("%d/%m/%Y")
    
    if not data_ref:
        return 1.0  # neutro se data inválida

    for tentativa in range(tentativas):
        try:
            url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={data_ref}&dataFinal={data_final}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            dados = r.json()
            
            if not dados:  # API retornou lista vazia → período futuro ou sem dados
                return 1.0
                
            fatores = [1 + float(d["valor"].replace(",", ".")) / 100 for d in dados]
            resultado = round(reduce(mul, fatores, 1), 8)
            
            # Só força 1.0 quando o checkbox está marcado
            if considerar_negativo and resultado < 1:
                return 1.0
                
            return resultado
            
        except Exception:
            time.sleep(delay)
    
    # Após todas tentativas falharem (ex: erro de rede persistente)
    return 1.0

def obter_ipca(d, considerar_negativo=False):   
    return consultar_bacen_fator(433, converter_data_para_bacen(d), considerar_negativo=considerar_negativo)

def obter_igpm(d, considerar_negativo=False):   
    return consultar_bacen_fator(189, converter_data_para_bacen(d), considerar_negativo=considerar_negativo)

def obter_igpdi(d, considerar_negativo=False):  
    return consultar_bacen_fator(190, converter_data_para_bacen(d), considerar_negativo=considerar_negativo)

def converter_data_para_bacen(valor):
    try:
        if pd.isna(valor) or valor == "" or valor is None:
            return None
        data = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(data):
            return None
        return data.strftime("%d/%m/%Y")
    except Exception:
        return None

def extrair_tabela_pdf(uploaded_file):
    tabelas = []
    with pdfplumber.open(uploaded_file) as pdf:
        for page in pdf.pages:
            tabela = page.extract_table({"vertical_strategy": "lines", "horizontal_strategy": "lines"})
            if not tabela or len(tabela) < 2: continue
            df = pd.DataFrame(tabela)
            df.dropna(how="all", inplace=True)
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)
            tabelas.append(df)
    if not tabelas:
        raise Exception("Nenhuma tabela encontrada")
    return pd.concat(tabelas, ignore_index=True)

def corrigir_tipos(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    if "Número de Postes" in df.columns:
        df["Número de Postes"] = pd.to_numeric(df["Número de Postes"], errors="coerce").fillna(0).astype(int)
    for col in ["Preço que estava sendo cobrado pela CEMIG", "Preço conquistado na AÇÃO",
                "Valor conquistado na AÇÃO", "Valor CEMIG", "Benefício Econômico"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    for col in ["IPCA", "IGPM", "IGPDI", "ICGJ", "Corrigido IPCA", "Corrigido IGPM", "Corrigido IGPDI", "Corrigido ICGJ",
                "Honorários IPCA", "Honorários IGPM", "Honorários IGPDI", "Honorários ICGJ"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


# =====================================================
# STREAMLIT APP
# =====================================================
st.set_page_config(page_title="Correção Monetária - ERP", layout="wide")
st.title("🏢 Automação para Correção Monetária")

# Session State
if "raw_df" not in st.session_state: st.session_state.raw_df = None
if "df" not in st.session_state: st.session_state.df = None
if "df_original" not in st.session_state: st.session_state.df_original = None
if "references" not in st.session_state: st.session_state.references = [""]

if "current_step" not in st.session_state:
    st.session_state.current_step = "1️⃣ Passo 1 - Extrair PDF"

# ====================== BARRA DE NAVEGAÇÃO ======================
st.markdown("### 📍 Passos do Processo")
col1, col2, col3, col4, col5 = st.columns(5, gap="small")

steps = [
    ("1️⃣ Passo 1 - Extrair PDF", "1️⃣"),
    ("2️⃣ Passo 2 - Processar Colunas", "2️⃣"),
    ("3️⃣ Passo 3 - Projetar Parcelas", "3️⃣"),
    ("4️⃣ Passo 4 - Parâmetros da Análise", "4️⃣"),
    ("5️⃣ Passo 5 - Consultar BACEN & Exportar", "5️⃣")
]

current_step = st.session_state.get("current_step", "1️⃣ Passo 1 - Extrair PDF")

for i, (label, emoji) in enumerate(steps):
    with [col1, col2, col3, col4, col5][i]:
        if st.button(label, width="stretch",
                     type="primary" if current_step.startswith(emoji) else "secondary"):
            st.session_state.current_step = label
            st.rerun()

st.divider()

# ====================== PASSO 1 ======================
if current_step.startswith("1️⃣"):
    st.subheader("1️⃣ Passo 1 - Extrair PDF")
    uploaded_file = st.file_uploader("Selecione o arquivo PDF da planilha CEMIG", type=["pdf"])
    
    if st.button("Extrair Tabelas do PDF", type="primary", width="stretch"):
        if uploaded_file:
            try:
                with st.spinner("Extraindo tabelas do PDF..."):
                    st.session_state.raw_df = extrair_tabela_pdf(uploaded_file)
                    st.session_state.df = st.session_state.raw_df.copy()
                    st.session_state.df_original = st.session_state.raw_df.copy()
                st.success(f"✅ {len(st.session_state.raw_df)} registros extraídos com sucesso!")
            except Exception as e:
                st.error(f"Erro na extração: {e}")

# ====================== PASSO 2 - PROCESSAR COLUNAS ======================
elif current_step.startswith("2️⃣"):
    st.subheader("2️⃣ Passo 2 - Processar Colunas")
    if st.session_state.raw_df is None:
        st.warning("Volte ao Passo 1 e extraia o PDF primeiro.")
    else:
        if st.button("Processar Colunas e Cálculos Iniciais", type="primary", width="stretch"):
            try:
                with st.spinner("Processando..."):
                    df = st.session_state.raw_df.copy()

                    for standard, variants in COLUMN_VARIANTS.items():
                        for col in list(df.columns):
                            if col in variants:
                                df.rename(columns={col: standard}, inplace=True)
                                break

                    df["Número de Postes"] = df.get("Pontos", pd.Series()).apply(tratar_postes)
                    col_preco = "Preço Pto."
                    df["Preço que estava sendo cobrado pela CEMIG"] = df.get(col_preco, pd.Series()).apply(tratar_numero)
                    df["Preço conquistado na AÇÃO"] = df.get(col_preco, pd.Series()).apply(tratar_numero)
                    df["Valor conquistado na AÇÃO"] = (df["Número de Postes"] * df["Preço conquistado na AÇÃO"]).round(2)
                    df["Valor CEMIG"] = (df["Número de Postes"] * df["Preço que estava sendo cobrado pela CEMIG"]).round(2)
                    df["Benefício Econômico"] = (df["Valor CEMIG"] - df["Valor conquistado na AÇÃO"]).round(2)

                    df["referencia"] = df["Mês referência/Ano cobrança"].apply(converter_referencia_yyyymm)
                    df["referencia_pgto"] = df.get("Pagamento", pd.Series()).apply(converter_referencia_yyyymm_pagamento)

                    df["referencia_vcto"] = df.get("Vencimento", pd.Series()).apply(converter_referencia_yyyymm_pagamento)

                    df = df[df["Mês referência/Ano cobrança"].apply(eh_mes_ano) & (df["referencia"] != "")].reset_index(drop=True)

                    df = recalcular_icgj(df)

                    for c in ["IPCA","IGPM","IGPDI"]: df[c] = pd.Series(dtype=float)
                    for c in ["Corrigido IPCA","Corrigido IGPM","Corrigido IGPDI","Corrigido ICGJ"]: df[c] = 0.0
                    for c in ["Honorários IPCA","Honorários IGPM","Honorários IGPDI","Honorários ICGJ"]: df[c] = 0.0

                    col_order = ["Mês referência/Ano cobrança","referencia","Pagamento","referencia_pgto","Vencimento","referencia_vcto","Número de Postes",
                                 "Preço que estava sendo cobrado pela CEMIG","Valor CEMIG","Preço conquistado na AÇÃO",
                                 "Valor conquistado na AÇÃO","Benefício Econômico",
                                 "IPCA","IGPM","IGPDI","ICGJ","Corrigido IPCA","Corrigido IGPM","Corrigido IGPDI","Corrigido ICGJ",
                                 "Honorários IPCA","Honorários IGPM","Honorários IGPDI","Honorários ICGJ"]
                    df = df[[c for c in col_order if c in df.columns]]
                    df = df.sort_values("referencia").reset_index(drop=True)

                    st.session_state.df = corrigir_tipos(df)
                    st.session_state.df_original = st.session_state.df.copy()

                    refs = df.drop_duplicates(subset=["Mês referência/Ano cobrança"], keep="first")["Mês referência/Ano cobrança"].tolist()
                    st.session_state.references = [""] + refs

                st.success(f"✅ Processamento concluído! {len(df)} registros gerados.")
            except Exception as e:
                st.error(f"Erro no processamento: {e}")

# ====================== PASSO 3 - PROJETAR PARCELAS ======================
elif current_step.startswith("3️⃣"):
    st.subheader("3️⃣ Passo 3 - Projetar Parcelas")

    if st.session_state.df is None or st.session_state.df.empty:
        st.warning("Conclua o Passo 2 primeiro.")
    else:
        st.markdown("##### Adicionar parcelas futuras automaticamente")

        st.write("**Data do Trânsito em Julgado:**")
        col_date, _ = st.columns([1.2, 2.8])
        with col_date:
            data_transito = st.date_input("Data do Trânsito em Julgado", label_visibility="collapsed")

        st.write("**Quantidade de novas parcelas a projetar:**")
        col_qty, col_add, col_del = st.columns([1, 1, 1])
        with col_qty:
            quantidade = st.number_input("Quantidade de novas parcelas", min_value=1, value=12, step=1, label_visibility="collapsed")
        with col_add:
            if st.button("➕ Adicionar", type="primary", width="stretch"):
                try:
                    # Converter data de trânsito para formato YYYYMM
                    referencia_transito = data_transito.strftime("%Y%m")
                    
                    ultima_linha = st.session_state.df.iloc[-1].copy()

                    novas_linhas = []
                    referencia_atual = referencia_transito

                    for i in range(quantidade):
                        if i == 0:
                            # Primeira parcela começa na data de trânsito
                            pass
                        else:
                            referencia_atual = add_meses_yyyymm(referencia_atual, 1)
                        nova_linha = ultima_linha.copy()
                        mes_nome = list(MESES.keys())[int(referencia_atual[4:]) - 1]
                        ano = referencia_atual[:4]
                        nova_linha["Mês referência/Ano cobrança"] = f"{mes_nome}/{ano}"
                        nova_linha["referencia"] = referencia_atual
                        nova_linha["Pagamento"] = ""
                        nova_linha["referencia_pgto"] = ""
                        nova_linha["Vencimento"] = ""
                        nova_linha["referencia_vcto"] = ""

                        for col in ["IPCA", "IGPM", "IGPDI", "Corrigido IPCA", "Corrigido IGPM", "Corrigido IGPDI", "Corrigido ICGJ",
                                    "Honorários IPCA", "Honorários IGPM", "Honorários IGPDI", "Honorários ICGJ"]:
                            if col in nova_linha:
                                nova_linha[col] = 0.0

                        novas_linhas.append(nova_linha)

                    df_novo = pd.DataFrame(novas_linhas)
                    st.session_state.df = pd.concat([st.session_state.df, df_novo], ignore_index=True)
                    st.session_state.df = st.session_state.df.sort_values("referencia").reset_index(drop=True)

                    st.session_state.df = recalcular_icgj(st.session_state.df)
                    st.session_state.df = corrigir_tipos(st.session_state.df)

                    st.success(f"✅ {quantidade} novas parcelas adicionadas com ICGJ atualizado!")
                    st.rerun()

                except Exception as e:
                    st.error(f"Erro ao adicionar parcelas: {e}")

        with col_del:
            if st.button("🗑️ Excluir Todas as Parcelas Projetadas", type="secondary", width="stretch"):
                if len(st.session_state.df) > len(st.session_state.df_original):
                    st.session_state.df = st.session_state.df_original.copy().reset_index(drop=True)
                    st.session_state.df = corrigir_tipos(st.session_state.df)
                    st.success("✅ Todas as parcelas projetadas foram excluídas!")
                    st.rerun()
                else:
                    st.info("Não há parcelas projetadas para excluir.")

# ====================== PASSO 4 - PARÂMETROS DA ANÁLISE ======================
elif current_step.startswith("4️⃣"):
    st.subheader("4️⃣ Passo 4 - Parâmetros da Análise")
    if st.session_state.df is None:
        st.warning("Conclua o Passo 2 primeiro.")
    else:
        considerar_negativo = st.session_state.get("considerar_neg_global", False)
        st.divider()

        tab1, tab2, tab3 = st.tabs(["🔍 Filtro Geral", "💰 Fornecedor (CEMIG)", "✅ Valor Conquistado"])

        with tab1:
            st.markdown("##### Filtrar dados a partir de uma referência específica")
            col_select, _ = st.columns([1.2, 2.8])
            with col_select:
                filtro_referencia = st.selectbox("Referência inicial:", st.session_state.references, key="filtro_ref")
            
            col_button, _ = st.columns([1.2, 2.8])
            with col_button:
                if st.button("🔄 Aplicar Filtro", type="secondary", width="stretch"):
                    if filtro_referencia:
                        ref_base = converter_referencia_yyyymm(filtro_referencia)
                        df_filtrado = st.session_state.df[st.session_state.df["referencia"] >= ref_base].copy().reset_index(drop=True)

                        df_anterior = st.session_state.df[st.session_state.df["referencia"] < ref_base].sort_values("referencia")
                        if not df_anterior.empty:
                            preco_congelado = df_anterior.iloc[-1]["Preço que estava sendo cobrado pela CEMIG"]
                            df_filtrado["Preço que estava sendo cobrado pela CEMIG"] = preco_congelado
                            df_filtrado["Valor CEMIG"] = (df_filtrado["Número de Postes"] * preco_congelado).round(2)
                            df_filtrado["Benefício Econômico"] = (df_filtrado["Valor CEMIG"] - df_filtrado["Valor conquistado na AÇÃO"]).round(2)

                        st.session_state.df = df_filtrado.sort_values("referencia").reset_index(drop=True)
                        st.session_state.df = corrigir_tipos(st.session_state.df)
                        st.success(f"✅ Filtro aplicado!")

        with tab2:
            st.markdown("##### Configurar preço do fornecedor (CEMIG)")
            col_m, col_v, col_i = st.columns([1.2, 1, 1.3])
            with col_m:
                marco_fornecedor = st.selectbox("📅 Marco Fornecedor:", st.session_state.references, key="marco_forn")
            with col_v:
                valor_fornecedor = st.number_input("💵 Valor (R$):", value=0.0, format="%.4f", key="val_forn")
            with col_i:
                indice_fornecedor = st.selectbox("📊 Índice:", ["IPCA", "IGPM", "IGP-DI", "ICGJ", "Outros"], key="ind_forn")
            
            if st.button("🚀 Atualizar Fornecedor", type="primary", width="stretch"):
                if valor_fornecedor <= 0:
                    st.warning("❌ Valor do Fornecedor deve ser maior que zero.")
                else:
                    marco_yyyymm = converter_referencia_yyyymm(marco_fornecedor)
                    preco_atual = valor_fornecedor
                    bloco_anterior = 0
                    with st.spinner("Atualizando preço FORNECEDOR..."):
                        for i, row in st.session_state.df.iterrows():
                            linha_yyyymm = row["referencia"]

                            meses_diff = calcular_meses_diff(marco_yyyymm, linha_yyyymm)
                            bloco_atual = meses_diff // 12
                            if bloco_atual > bloco_anterior:
                                inicio = add_meses_yyyymm(marco_yyyymm, bloco_anterior * 12)
                                fim = add_meses_yyyymm(marco_yyyymm, bloco_atual * 12)
                                data_inicio = yyyymm_para_data_bacen(inicio)
                                data_fim = yyyymm_para_data_bacen(fim)
                                if data_inicio and data_fim:
                                    if indice_fornecedor == "IPCA":
                                        fator = consultar_bacen_fator(433, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_fornecedor == "IGPM":
                                        fator = consultar_bacen_fator(189, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_fornecedor == "IGP-DI":
                                        fator = consultar_bacen_fator(190, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_fornecedor == "ICGJ":
                                        fator = 1 + (row.get("ICGJ", 0)/100)
                                        if considerar_negativo and fator < 1:
                                            fator = 1.0
                                    else:
                                        fator = 1.0

                                    preco_atual *= fator
                                    bloco_anterior = bloco_atual

                            st.session_state.df.at[i, "Preço que estava sendo cobrado pela CEMIG"] = round(preco_atual, 2)
                            rounded_preco_forn = round(preco_atual, 2)
                            st.session_state.df.at[i, "Valor CEMIG"] = round(st.session_state.df.at[i, "Número de Postes"] * rounded_preco_forn, 2)
                            st.session_state.df.at[i, "Benefício Econômico"] = round(st.session_state.df.at[i, "Valor CEMIG"] - st.session_state.df.at[i, "Valor conquistado na AÇÃO"], 2)
                    st.session_state.df = st.session_state.df.sort_values("referencia").reset_index(drop=True)
                    st.session_state.df = corrigir_tipos(st.session_state.df)
                    st.success("✅ Preço FORNECEDOR atualizado com sucesso!")

        with tab3:
            st.markdown("##### Configurar preço conquistado na ação")
            col_m2, col_v2, col_i2 = st.columns([1.2, 1, 1.3])
            with col_m2:
                marco_conquistado = st.selectbox("📅 Marco Conquistado:", st.session_state.references, key="marco_conq")
            with col_v2:
                valor_conquistado = st.number_input("💵 Valor (R$):", value=0.0, format="%.4f", key="val_conq")
            with col_i2:
                indice_contrato = st.selectbox("📊 Índice:", ["IPCA", "IGPM", "IGP-DI", "ICGJ", "Outros"], key="ind_conq")
            
            if st.button("🚀 Atualizar Conquistado", type="primary", width="stretch"):
                if valor_conquistado <= 0:
                    st.warning("❌ Valor conquistado deve ser maior que zero.")
                else:
                    marco_yyyymm = converter_referencia_yyyymm(marco_conquistado)
                    preco_atual = valor_conquistado
                    bloco_anterior = 0
                    with st.spinner("Atualizando preço CONQUISTADO..."):
                        for i, row in st.session_state.df.iterrows():
                            linha_yyyymm = row["referencia"]

                            meses_diff = calcular_meses_diff(marco_yyyymm, linha_yyyymm)
                            bloco_atual = meses_diff // 12
                            if bloco_atual > bloco_anterior:
                                inicio = add_meses_yyyymm(marco_yyyymm, bloco_anterior * 12)
                                fim = add_meses_yyyymm(marco_yyyymm, bloco_atual * 12)
                                data_inicio = yyyymm_para_data_bacen(inicio)
                                data_fim = yyyymm_para_data_bacen(fim)
                                if data_inicio and data_fim:
                                    if indice_contrato == "IPCA":
                                        fator = consultar_bacen_fator(433, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_contrato == "IGPM":
                                        fator = consultar_bacen_fator(189, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_contrato == "IGP-DI":
                                        fator = consultar_bacen_fator(190, data_inicio, data_fim, considerar_negativo=considerar_negativo)
                                    elif indice_contrato == "ICGJ":
                                        fator = 1 + (row.get("ICGJ", 0)/100)
                                        if considerar_negativo and fator < 1:
                                            fator = 1.0
                                    else:
                                        fator = 1.0

                                    preco_atual *= fator
                                    bloco_anterior = bloco_atual

                            st.session_state.df.at[i, "Preço conquistado na AÇÃO"] = round(preco_atual, 2)
                            rounded_preco_conq = round(preco_atual, 2)
                            st.session_state.df.at[i, "Valor conquistado na AÇÃO"] = round(st.session_state.df.at[i, "Número de Postes"] * rounded_preco_conq, 2)
                            st.session_state.df.at[i, "Benefício Econômico"] = round(st.session_state.df.at[i, "Valor CEMIG"] - st.session_state.df.at[i, "Valor conquistado na AÇÃO"], 2)
                    st.session_state.df = st.session_state.df.sort_values("referencia").reset_index(drop=True)
                    st.session_state.df = corrigir_tipos(st.session_state.df)
                    st.success("✅ Preço CONQUISTADO atualizado com sucesso!")

# ====================== PASSO 5 ======================
elif current_step.startswith("5️⃣"):
    st.subheader("5️⃣ Passo 5 - Consultar BACEN & Exportar")
    st.checkbox("Padronizar Índices Negativos para 1.0?", value=st.session_state.get("considerar_neg_global", False), key="considerar_neg_global")
    
    col_num, col_cons, col_exp = st.columns([1, 1, 1])
    with col_num:
        st.write("**Honorários (%):**")
        percentual_honorarios = st.number_input("Percentual de Honorários", min_value=0.0, max_value=100.0, value=0.0, step=0.01, key="percentual_honorarios", label_visibility="collapsed")
    with col_cons:
        st.write("**Consultar dados no BACEN:**")
        if st.button("🔍 BACEN", type="secondary", width="stretch"):
            if st.session_state.df is not None:
                    with st.spinner("Consultando BACEN..."):
                        considerar_neg = st.session_state.get("considerar_neg_global", False)
                        percentual_honorarios = st.session_state.get("percentual_honorarios", 0.0)

                        for i, row in st.session_state.df.iterrows():
                            ref_pgto = row.get("referencia_pgto")
                            if pd.isna(ref_pgto) or ref_pgto == "" or ref_pgto == 0:
                                data_para_consulta = yyyymm_para_data_bacen(row["referencia"])
                            else:
                                data_para_consulta = row.get("Pagamento")

                            ipca  = obter_ipca (data_para_consulta, considerar_negativo=considerar_neg)
                            igpm  = obter_igpm (data_para_consulta, considerar_negativo=considerar_neg)
                            igpdi = obter_igpdi(data_para_consulta, considerar_negativo=considerar_neg)

                            # Forçamento explícito e definitivo quando checkbox marcado
                            ipca_final  = 1.0 if (considerar_neg and ipca is not None and ipca < 1) else ipca
                            igpm_final  = 1.0 if (considerar_neg and igpm is not None and igpm < 1) else igpm
                            igpdi_final = 1.0 if (considerar_neg and igpdi is not None and igpdi < 1) else igpdi

                            st.session_state.df.at[i, "IPCA"]  = ipca_final
                            st.session_state.df.at[i, "IGPM"]  = igpm_final
                            st.session_state.df.at[i, "IGPDI"] = igpdi_final

                            benef = st.session_state.df.at[i, "Benefício Econômico"]

                            if ipca_final is not None:
                                st.session_state.df.at[i, "Corrigido IPCA"] = round(benef * ipca_final, 2)
                            if igpm_final is not None:
                                st.session_state.df.at[i, "Corrigido IGPM"] = round(benef * igpm_final, 2)
                            if igpdi_final is not None:
                                st.session_state.df.at[i, "Corrigido IGPDI"] = round(benef * igpdi_final, 2)

                            icgj = st.session_state.df.at[i, "ICGJ"]
                            if pd.notna(icgj) and icgj != 0:
                                icgj_final = 1.0 if (considerar_neg and icgj < 1) else icgj
                                st.session_state.df.at[i, "ICGJ"] = icgj_final
                                st.session_state.df.at[i, "Corrigido ICGJ"] = round(benef * icgj_final, 2)

                            # Calcular Honorários
                            percentual = percentual_honorarios / 100
                            st.session_state.df.at[i, "Honorários IPCA"] = round(st.session_state.df.at[i, "Corrigido IPCA"] * percentual, 2) if pd.notna(st.session_state.df.at[i, "Corrigido IPCA"]) else 0.0
                            st.session_state.df.at[i, "Honorários IGPM"] = round(st.session_state.df.at[i, "Corrigido IGPM"] * percentual, 2) if pd.notna(st.session_state.df.at[i, "Corrigido IGPM"]) else 0.0
                            st.session_state.df.at[i, "Honorários IGPDI"] = round(st.session_state.df.at[i, "Corrigido IGPDI"] * percentual, 2) if pd.notna(st.session_state.df.at[i, "Corrigido IGPDI"]) else 0.0
                            st.session_state.df.at[i, "Honorários ICGJ"] = round(st.session_state.df.at[i, "Corrigido ICGJ"] * percentual, 2) if pd.notna(st.session_state.df.at[i, "Corrigido ICGJ"]) else 0.0

                        # Forçamento de segurança FINAL (após todo o loop)
                        considerar_neg = st.session_state.get("considerar_neg_global", False)
                        for col in ["IPCA", "IGPM", "IGPDI", "ICGJ"]:
                            if col in st.session_state.df.columns:
                                st.session_state.df[col] = st.session_state.df[col].apply(
                                    lambda v: 1.0 if (considerar_neg and pd.notna(v) and v < 1) else v
                                )

                        st.session_state.df = corrigir_tipos(st.session_state.df)

                        # TOTAL NO FINAL
                        total_row = {col: "" for col in st.session_state.df.columns}
                        total_row["Mês referência/Ano cobrança"] = "TOTAL"
                        for col in ["Valor conquistado na AÇÃO", "Valor CEMIG", "Benefício Econômico",
                                    "Corrigido IPCA", "Corrigido IGPM", "Corrigido IGPDI", "Corrigido ICGJ",
                                    "Honorários IPCA", "Honorários IGPM", "Honorários IGPDI", "Honorários ICGJ"]:
                            if col in st.session_state.df.columns:
                                total_row[col] = round(st.session_state.df[col].sum(), 2)

                        df_sem_total = st.session_state.df[st.session_state.df["Mês referência/Ano cobrança"] != "TOTAL"].copy()
                        st.session_state.df = pd.concat([df_sem_total, pd.DataFrame([total_row])], ignore_index=True)
                        st.session_state.df = corrigir_tipos(st.session_state.df)

                    st.success("✅ Consulta BACEN concluída!")

    with col_exp:
        st.write("**Exportar tabela final para excel:**")
        if st.button("📥 Excel", type="primary", width="stretch"):
            if st.session_state.df is not None:
                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    st.session_state.df.to_excel(writer, index=False)
                output.seek(0)
                st.download_button("📥 Baixar Excel", output, "resultado_correcao_monetaria.xlsx",
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ====================== TABELA FINAL ======================
st.divider()
st.subheader("📋 Tabela Atual (atualizada em tempo real)")

if st.session_state.get("df") is not None and not st.session_state.df.empty:
    
    df = st.session_state.df
    
    # Colunas-alvo
    colunas_indices = ["IPCA", "IGPM", "IGPDI", "ICGJ"]
    cols_exist = [c for c in colunas_indices if c in df.columns]
    
    styler = df.style
    
    if cols_exist:
        def pintar_vermelho(v):
            try:
                if float(v) < 1:
                    return "background-color: #ffdddd;"
                return ""
            except:
                return ""
        
        styler = styler.map(
            pintar_vermelho,
            subset=cols_exist
        )
        
        # Formatação numérica (opcional, mas ajuda muito)
        styler = styler.format(
            precision=4,
            subset=cols_exist,
            na_rep="-"
        )
    
    # Opcional: destacar linha TOTAL se existir
    def destacar_total(row):
        if row.get("Mês referência/Ano cobrança") == "TOTAL":
            return ["font-weight: bold; background-color: #f8f9fa;"] * len(row)
        return [""] * len(row)
    
    styler = styler.apply(destacar_total, axis=1)
    
    st.dataframe(
        styler,
        width="stretch",
        height=720,
        hide_index=True
    )

elif st.session_state.get("raw_df") is not None and not st.session_state.raw_df.empty:
    st.dataframe(
        st.session_state.raw_df,
        width="stretch",
        height=720,
        hide_index=True
    )

else:
    st.info("Extraia o PDF no Passo 1 para começar.")

st.caption("Versão: v.1.1")
