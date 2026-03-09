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
    "Preço Pto.": ["Preço Pto.", "Prç.Pto./Vlr."]
}

# =====================================================
# UTILITÁRIOS (100% iguais ao Tkinter + melhorias do segundo código)
# =====================================================
def yyyymm_para_data_bacen(yyyymm):
    """ Converte '202012' → '01/12/2020' """
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
    if pd.isna(valor): return ""
    texto = str(valor).replace(" ", "").replace(".", "").replace(",", "")
    return texto if texto.isdigit() else ""

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
        caminho = os.path.join(base_dir, "ICGJ.CSV")

        df = pd.read_csv(caminho)

        df["Referencia_yyyymm"] = df["Referencia_yyyymm"].astype(str)
        df["Indice"] = df["Indice"].astype(str).str.replace(",", ".").astype(float)

        return df[["Referencia_yyyymm", "Indice"]]

    except Exception as e:
        st.error(f"Erro ao carregar ICGJ.CSV: {e}")
        return pd.DataFrame(columns=["Referencia_yyyymm", "Indice"])
        

def consultar_bacen_fator(codigo, data_ref, data_final=None, tentativas=3, delay=3):
    if data_final is None: data_final = datetime.today().strftime("%d/%m/%Y")
    if not data_ref: return None
    for tentativa in range(tentativas):
        try:
            url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={data_ref}&dataFinal={data_final}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            dados = r.json()
            if not dados: return None
            fatores = [1 + float(d["valor"].replace(",", ".")) / 100 for d in dados]
            return round(reduce(mul, fatores, 1), 8)
        except Exception:
            time.sleep(delay)
    return None

def obter_ipca(d):   return consultar_bacen_fator(433, converter_data_para_bacen(d))
def obter_igpm(d):   return consultar_bacen_fator(189, converter_data_para_bacen(d))
def obter_igpdi(d):  return consultar_bacen_fator(190, converter_data_para_bacen(d))

def converter_data_para_bacen(valor):
    try:
        if pd.isna(valor): return None
        data = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        return data.strftime("%d/%m/%Y") if pd.notna(data) else None
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

# =====================================================
# STREAMLIT APP - LAYOUT + TODAS AS FUNCIONALIDADES DO TKINTER
# =====================================================
st.set_page_config(page_title="Correção Monetária - ERP", layout="wide")
st.title("🏢 Automação para Correção Monetária (versão - 1.0")

# Session State
if "raw_df" not in st.session_state: st.session_state.raw_df = None
if "df" not in st.session_state: st.session_state.df = None
if "df_original" not in st.session_state: st.session_state.df_original = None
if "references" not in st.session_state: st.session_state.references = [""]
if "current_step" not in st.session_state: st.session_state.current_step = "1️⃣ Passo 1 - Extrair PDF"

# ====================== BARRA DE NAVEGAÇÃO HORIZONTAL ======================
st.markdown("### 📍 Passos do Processo")
col1, col2, col3, col4 = st.columns(4, gap="small")

steps = [
    ("1️⃣ Passo 1 - Extrair PDF", "1️⃣"),
    ("2️⃣ Passo 2 - Processar Colunas", "2️⃣"),
    ("3️⃣ Passo 3 - Parâmetros da Análise", "3️⃣"),
    ("4️⃣ Passo 4 - Consultar BACEN & Exportar", "4️⃣")
]

current_step = st.session_state.get("current_step", "1️⃣ Passo 1 - Extrair PDF")

with col1:
    if st.button(steps[0][0], use_container_width=True, type=("secondary" if not current_step.startswith(steps[0][1]) else "primary")):
        st.session_state.current_step = steps[0][0]
        st.rerun()

with col2:
    if st.button(steps[1][0], use_container_width=True, type=("secondary" if not current_step.startswith(steps[1][1]) else "primary")):
        st.session_state.current_step = steps[1][0]
        st.rerun()

with col3:
    if st.button(steps[2][0], use_container_width=True, type=("secondary" if not current_step.startswith(steps[2][1]) else "primary")):
        st.session_state.current_step = steps[2][0]
        st.rerun()

with col4:
    if st.button(steps[3][0], use_container_width=True, type=("secondary" if not current_step.startswith(steps[3][1]) else "primary")):
        st.session_state.current_step = steps[3][0]
        st.rerun()

st.divider()
current_step = st.session_state.get("current_step", "1️⃣ Passo 1 - Extrair PDF")

# ====================== PASSO 1 ======================
if current_step.startswith("1️⃣"):
    st.subheader("1️⃣ Passo 1 - Extrair PDF")
    uploaded_file = st.file_uploader("Selecione o arquivo PDF da planilha CEMIG", type=["pdf"])
    
    if st.button("Extrair Tabelas do PDF", type="primary", use_container_width=True):
        if uploaded_file:
            try:
                with st.spinner("Extraindo tabelas do PDF..."):
                    st.session_state.raw_df = extrair_tabela_pdf(uploaded_file)
                    st.session_state.df = st.session_state.raw_df.copy()
                    st.session_state.df_original = st.session_state.raw_df.copy()
                st.success(f"✅ {len(st.session_state.raw_df)} registros extraídos com sucesso!")
            except Exception as e:
                st.error(f"Erro na extração: {e}")

# ====================== PASSO 2 ======================
elif current_step.startswith("2️⃣"):
    st.subheader("2️⃣ Passo 2 - Processar Colunas")
    if st.session_state.raw_df is None:
        st.warning("Volte ao Passo 1 e extraia o PDF primeiro.")
    else:
        if st.button("Processar Colunas e Cálculos Iniciais", type="primary", use_container_width=True):
            try:
                with st.spinner("Processando..."):
                    df = st.session_state.raw_df.copy()

                    # Renomear colunas (igual ao Tkinter)
                    for standard, variants in COLUMN_VARIANTS.items():
                        for col in list(df.columns):
                            if col in variants:
                                df.rename(columns={col: standard}, inplace=True)
                                break

                    df["Número de Postes"] = df.get("Pontos", pd.Series()).apply(tratar_postes).apply(lambda x: int(x) if str(x).isdigit() else 0)
                    col_preco = "Preço Pto."
                    df["Preço que estava sendo cobrado pela CEMIG"] = df.get(col_preco, pd.Series()).apply(tratar_numero)
                    df["Preço conquistado na AÇÃO"] = df.get(col_preco, pd.Series()).apply(tratar_numero)
                    df["Valor conquistado na AÇÃO"] = (df["Número de Postes"] * df["Preço conquistado na AÇÃO"]).round(2)
                    df["Valor CEMIG"] = (df["Número de Postes"] * df["Preço que estava sendo cobrado pela CEMIG"]).round(2)
                    df["Benefício Econômico"] = (df["Valor CEMIG"] - df["Valor conquistado na AÇÃO"]).round(2)

                    df["referencia"] = df["Mês referência/Ano cobrança"].apply(converter_referencia_yyyymm)
                    df["referencia_pgto"] = df.get("Pagamento", pd.Series()).apply(converter_referencia_yyyymm_pagamento)

                    df = df[df["Mês referência/Ano cobrança"].apply(eh_mes_ano) & (df["referencia"] != "")].reset_index(drop=True)

                    df = df.merge(carregar_icgj(), left_on="referencia_pgto", right_on="Referencia_yyyymm", how="left")
                    df.rename(columns={"Indice": "ICGJ"}, inplace=True)
                    df.drop(columns=["Referencia_yyyymm"], errors="ignore", inplace=True)

                    for c in ["IPCA","IGPM","IGPDI"]: df[c] = pd.Series(dtype=float)
                    df["ICGJ"] = df["ICGJ"].fillna(0.0)
                    for c in ["Corrigido IPCA","Corrigido IGPM","Corrigido IGPDI","Corrigido ICGJ"]: df[c] = 0.0

                    col_order = ["Mês referência/Ano cobrança","referencia","Pagamento","referencia_pgto","Número de Postes",
                                 "Preço que estava sendo cobrado pela CEMIG","Preço conquistado na AÇÃO",
                                 "Valor conquistado na AÇÃO","Valor CEMIG","Benefício Econômico",
                                 "IPCA","IGPM","IGPDI","ICGJ","Corrigido IPCA","Corrigido IGPM","Corrigido IGPDI","Corrigido ICGJ"]
                    df = df[[c for c in col_order if c in df.columns]]
                    df = df.sort_values("referencia").reset_index(drop=True)

                    st.session_state.df = df
                    st.session_state.df_original = df.copy()

                    # Ordenar referências pela coluna "referencia" (YYYYMM) ao invés de alfabéticamente
                    refs = df.drop_duplicates(subset=["Mês referência/Ano cobrança"], keep="first")["Mês referência/Ano cobrança"].tolist()
                    st.session_state.references = [""] + refs

                st.success(f"✅ Processamento concluído! {len(df)} registros gerados.")
            except Exception as e:
                st.error(f"Erro no processamento: {e}")

# ====================== PASSO 3 - PARÂMETROS (LAYOUT OTIMIZADO) ======================
elif current_step.startswith("3️⃣"):
    st.subheader("3️⃣ Passo 3 - Parâmetros da Análise")
    if st.session_state.df is None:
        st.warning("Conclua o Passo 2 primeiro.")
    else:
        # Variável compartilhada para ambas as abas
        considerar_negativo = st.checkbox("✓ Considerar Índices Negativos em ambas as operações?", value=False, key="considerar_neg_global")
        
        st.divider()

        # TABS para separar as 3 seções
        tab1, tab2, tab3 = st.tabs(["🔍 Filtro Geral", "💰 Fornecedor (CEMIG)", "✅ Valor Conquistado"])

        # ==== TAB 1: FILTRO GERAL ====
        with tab1:
            st.markdown("##### Filtrar dados a partir de uma referência específica")
            
            # Selectbox mais compacto
            col_select, col_spacer = st.columns([1.2, 2.8])
            with col_select:
                filtro_referencia = st.selectbox("Referência inicial:", st.session_state.references, key="filtro_ref")
            
            # Botão na linha debaixo, alinhado à esquerda (seguindo padrão)
            col_button_left, col_spacer_right = st.columns([1.2, 2.8])
            with col_button_left:
                if st.button("🔄 Aplicar Filtro", type="secondary", use_container_width=True):
                    if filtro_referencia:
                        ref_base = converter_referencia_yyyymm(filtro_referencia)
                        df_original_count = len(st.session_state.df_original)
                        
                        df_filtrado = st.session_state.df_original[
                            (st.session_state.df_original["referencia_pgto"] != "") & 
                            (st.session_state.df_original["referencia"] >= ref_base)
                        ].copy().reset_index(drop=True)

                        # Lógica de congelamento do preço (exatamente como no Tkinter)
                        df_anterior = st.session_state.df_original[
                            (st.session_state.df_original["referencia_pgto"] != "") &
                            (st.session_state.df_original["referencia"] < ref_base)
                        ].sort_values("referencia")

                        if not df_anterior.empty:
                            preco_congelado = df_anterior.iloc[-1]["Preço que estava sendo cobrado pela CEMIG"]
                            df_filtrado["Preço que estava sendo cobrado pela CEMIG"] = preco_congelado
                            df_filtrado["Valor CEMIG"] = (df_filtrado["Número de Postes"] * preco_congelado).round(2)
                            df_filtrado["Benefício Econômico"] = (df_filtrado["Valor CEMIG"] - df_filtrado["Valor conquistado na AÇÃO"]).round(2)

                        st.session_state.df = df_filtrado.sort_values("referencia").reset_index(drop=True)
                        registros_filtrados = len(df_filtrado)
                        registros_excluidos = df_original_count - registros_filtrados
                        st.success(f"✅ Filtro aplicado! {registros_filtrados} registros considerados, {registros_excluidos} excluídos.")
                    else:
                        st.warning("Selecione uma referência válida.")

            with st.expander("ℹ️ Sobre o filtro", expanded=False):
                st.caption("Este filtro permite aplicar um corte temporal nos dados. Valores anteriores à referência selecionada serão congelados no último preço registrado (Price Freeze).")

        # ==== TAB 2: FORNECEDOR ====
        with tab2:
            st.markdown("##### Configurar preço do fornecedor (CEMIG)")
            
            # Linha 1: Marco Fornecedor | Valor | Índice
            col_m, col_v, col_i = st.columns([1.2, 1, 1.3])
            with col_m:
                marco_fornecedor = st.selectbox("📅 Marco Fornecedor:", st.session_state.references, key="marco_forn")
            with col_v:
                valor_fornecedor = st.number_input("💵 Valor (R$):", value=0.0, format="%.4f", key="val_forn")
            with col_i:
                indice_fornecedor = st.selectbox("📊 Índice:", ["IPCA", "IGPM", "IGP-DI", "ICGJ", "Outros"], key="ind_forn")
            
            # Linha 2: Botão de atualização (alinhado à esquerda)
            col_button_left, col_spacer_right = st.columns([1.2, 2.8])
            with col_button_left:
                if st.button("🚀 Atualizar Fornecedor", type="primary", use_container_width=True):
                    if valor_fornecedor <= 0:
                        st.warning("❌ Valor do Fornecedor deve ser maior que zero.")
                    else:
                        marco_yyyymm = converter_referencia_yyyymm(marco_fornecedor)
                        preco_atual = valor_fornecedor
                        bloco_anterior = 0
                        with st.spinner("Atualizando preço FORNECEDOR em blocos de 12 meses..."):
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
                                        if indice_fornecedor == "IPCA": fator = consultar_bacen_fator(433, data_inicio, data_fim)
                                        elif indice_fornecedor == "IGPM": fator = consultar_bacen_fator(189, data_inicio, data_fim)
                                        elif indice_fornecedor == "IGP-DI": fator = consultar_bacen_fator(190, data_inicio, data_fim)
                                        elif indice_fornecedor == "ICGJ": fator = 1 + (row.get("ICGJ", 0)/100)
                                        else: fator = 1.0

                                        if fator is None or fator == 0 or (fator < 1 and not considerar_negativo):
                                            fator = 1.0
                                        preco_atual *= fator
                                        bloco_anterior = bloco_atual

                                st.session_state.df.at[i, "Preço que estava sendo cobrado pela CEMIG"] = round(preco_atual, 2)
                                st.session_state.df.at[i, "Valor CEMIG"] = round(st.session_state.df.at[i, "Número de Postes"] * preco_atual, 2)
                                st.session_state.df.at[i, "Benefício Econômico"] = round(st.session_state.df.at[i, "Valor CEMIG"] - st.session_state.df.at[i, "Valor conquistado na AÇÃO"], 2)
                        st.session_state.df = st.session_state.df.sort_values("referencia").reset_index(drop=True)
                        st.success("✅ Preço FORNECEDOR atualizado com sucesso!")

            with st.expander("ℹ️ Instruções", expanded=False):
                st.caption("1. Selecione a data que servirá como marco inicial; 2. Insira o valor do fornecedor; 3. Escolha o índice de reajuste; 4. Se necessário, ative a opção de índices negativos.")

        # ==== TAB 3: VALOR CONQUISTADO ====
        with tab3:
            st.markdown("##### Configurar preço conquistado na ação")
            
            # Linha 1: Marco Conquistado | Valor | Índice
            col_m2, col_v2, col_i2 = st.columns([1.2, 1, 1.3])
            with col_m2:
                marco_conquistado = st.selectbox("📅 Marco Conquistado:", st.session_state.references, key="marco_conq")
            with col_v2:
                valor_conquistado = st.number_input("💵 Valor (R$):", value=0.0, format="%.4f", key="val_conq")
            with col_i2:
                indice_contrato = st.selectbox("📊 Índice:", ["IPCA", "IGPM", "IGP-DI", "ICGJ", "Outros"], key="ind_conq")
            
            # Linha 2: Botão de atualização (alinhado à esquerda)
            col_button_left, col_spacer_right = st.columns([1.2, 2.8])
            with col_button_left:
                if st.button("🚀 Atualizar Conquistado", type="primary", use_container_width=True):
                    if valor_conquistado <= 0:
                        st.warning("❌ Valor conquistado deve ser maior que zero.")
                    else:
                        marco_yyyymm = converter_referencia_yyyymm(marco_conquistado)
                        preco_atual = valor_conquistado
                        bloco_anterior = 0
                        with st.spinner("Atualizando preço CONQUISTADO em blocos de 12 meses..."):
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
                                        if indice_contrato == "IPCA": fator = consultar_bacen_fator(433, data_inicio, data_fim)
                                        elif indice_contrato == "IGPM": fator = consultar_bacen_fator(189, data_inicio, data_fim)
                                        elif indice_contrato == "IGP-DI": fator = consultar_bacen_fator(190, data_inicio, data_fim)
                                        elif indice_contrato == "ICGJ": fator = 1 + (row.get("ICGJ", 0)/100)
                                        else: fator = 1.0

                                        if fator is None or fator == 0 or (fator < 1 and not considerar_negativo):
                                            fator = 1.0
                                        preco_atual *= fator
                                        bloco_anterior = bloco_atual

                                st.session_state.df.at[i, "Preço conquistado na AÇÃO"] = round(preco_atual, 2)
                                st.session_state.df.at[i, "Valor conquistado na AÇÃO"] = round(st.session_state.df.at[i, "Número de Postes"] * preco_atual, 2)
                                st.session_state.df.at[i, "Benefício Econômico"] = round(st.session_state.df.at[i, "Valor CEMIG"] - st.session_state.df.at[i, "Valor conquistado na AÇÃO"], 2)
                        st.session_state.df = st.session_state.df.sort_values("referencia").reset_index(drop=True)
                        st.success("✅ Preço CONQUISTADO atualizado com sucesso!")

            with st.expander("ℹ️ Instruções", expanded=False):
                st.caption("1. Selecione a data que servirá como marco inicial; 2. Insira o valor conquistado; 3. Escolha o índice previsto no contrato; 4. Se necessário, ative a opção de índices negativos.")

# ====================== PASSO 4 ======================
elif current_step.startswith("4️⃣"):
    st.subheader("4️⃣ Passo 4 - Consultar BACEN & Exportar")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Consultar BACEN (IPCA / IGPM / IGP-DI)", type="secondary", use_container_width=True):
            if st.session_state.df is not None:
                with st.spinner("Consultando BACEN..."):
                    for i, row in st.session_state.df.iterrows():
                        ref_pgto = row["Pagamento"]
                        ipca = obter_ipca(ref_pgto)
                        igpm = obter_igpm(ref_pgto)
                        igpdi = obter_igpdi(ref_pgto)

                        st.session_state.df.at[i, "IPCA"] = ipca
                        st.session_state.df.at[i, "IGPM"] = igpm
                        st.session_state.df.at[i, "IGPDI"] = igpdi

                        benef = st.session_state.df.at[i, "Benefício Econômico"]
                        if ipca is not None:
                            st.session_state.df.at[i, "Corrigido IPCA"] = round(benef * ipca, 2)
                        if igpm is not None:
                            st.session_state.df.at[i, "Corrigido IGPM"] = round(benef * igpm, 2)
                        if igpdi is not None:
                            st.session_state.df.at[i, "Corrigido IGPDI"] = round(benef * igpdi, 2)

                        icgj = st.session_state.df.at[i, "ICGJ"]
                        if pd.notna(icgj) and icgj != 0:
                            st.session_state.df.at[i, "Corrigido ICGJ"] = round(benef * icgj, 2)

                    # TOTAL
                    total = {col: "" for col in st.session_state.df.columns}
                    total["Mês referência/Ano cobrança"] = "TOTAL"
                    for col in ["Valor conquistado na AÇÃO", "Valor CEMIG", "Benefício Econômico",
                                "Corrigido IPCA", "Corrigido IGPM", "Corrigido IGPDI", "Corrigido ICGJ"]:
                        if col in st.session_state.df.columns:
                            total[col] = round(st.session_state.df[col].sum(), 2)
                    st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame([total])], ignore_index=True)
                    st.session_state.df = st.session_state.df.sort_values("referencia", na_position="last").reset_index(drop=True)
                st.success("✅ Consulta BACEN concluída!")

    with col_b:
        if st.button("Exportar para Excel", type="primary", use_container_width=True):
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
if st.session_state.df is not None and not st.session_state.df.empty:
    st.dataframe(st.session_state.df, use_container_width=True, height=720, hide_index=True)
elif st.session_state.raw_df is not None:
    st.dataframe(st.session_state.raw_df, use_container_width=True, height=720, hide_index=True)
else:
    st.info("Extraia o PDF no Passo 1 para começar.")


