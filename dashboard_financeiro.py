# ==========================================
# DASHBOARD FINANCEIRO - CT-e (ESI)
# ==========================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
import re
import time
import numpy as np
from streamlit_autorefresh import st_autorefresh


# ------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# ------------------------------------------
st.set_page_config(
    page_title="Dashboard Financeiro - Transportes",
    page_icon="🚛",
    layout="wide"
)

AUTO_REFRESH_SECONDS = 900  # 15 minutos

# Componente de Auto-Refresh (Mantém o painel vivo)
count = st_autorefresh(interval=AUTO_REFRESH_SECONDS * 1000, key="fancylostcounter")


# ------------------------------------------
# CSS
# ------------------------------------------
st.markdown("""
<style>
/* Card do KPI */
div[data-testid="metric-container"] {
    background-color: #ffffff;
    border-radius: 14px;
    padding: 16px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.10);
    border: 1px solid #e5e7eb;
}
/* Texto pequeno (título) */
div[data-testid="metric-container"] label {
    color: #6b7280 !important;
    font-size: 14px;
    font-weight: 600;
    text-transform: uppercase;
}
/* Texto grande (valor) */
div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
    color: #111827 !important;
    font-size: 26px;
    font-weight: 800;
}
/* Delta (Comparação) */
div[data-testid="metric-container"] div[data-testid="stMetricDelta"] {
    font-weight: 600;
    font-size: 14px;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------
# SIDEBAR
# ------------------------------------------
st.sidebar.header("Configuração")

if "last_auto_refresh" in st.session_state:
    last_update = datetime.fromtimestamp(st.session_state.last_auto_refresh).strftime('%H:%M')
    st.sidebar.caption(f"🕒 Atualizado às {last_update}")

if st.sidebar.button("🔄 Forçar Atualização Agora"):
    st.cache_data.clear()
    st.rerun()

SUBDOMAIN = st.sidebar.text_input("Subdomínio", value="trf")
TOKEN = st.sidebar.text_input("Token", value="7f-z3i1jYgura6oQBDKdeDxu2jBMqwAH2jFRjbGd2JJz9CBUWENQYA", type="password")
DAYS_BACK = st.sidebar.slider("Buscar últimos (dias)", 30, 365, 120)

CONNECT_API = st.sidebar.checkbox("Conectar à API", value=True)

# ------------------------------------------
# CÁLCULO DO PERÍODO (FORA DO CACHE)
# ------------------------------------------
from datetime import timezone, timedelta, datetime

fuso_br = timezone(timedelta(hours=-3))
since_dt = datetime.now(fuso_br) - timedelta(days=DAYS_BACK)

# ------------------------------------------
# FUNÇÃO: LER XML DO CT-e
# ------------------------------------------
def parse_cte_xml(xml_string):
    try:
        if not xml_string: return None

        # Limpeza
        xml_string = re.sub(r'xmlns[^=]*="[^"]*"', '', xml_string)
        xml_string = re.sub(r'(<\/?)\w+:', r'\1', xml_string)

        root = ET.fromstring(xml_string)
        inf = root.find(".//infCte")
        if inf is None: return None

        ide = inf.find("ide")
        numero = ide.findtext("nCT", default="S/N")
        dh_emi = ide.findtext("dhEmi")

        # Data com Fuso
        data = pd.to_datetime(dh_emi, errors="coerce")
        if data is not None:
            # Garante remoção de TZ para comparar com datas locais
            data = data.replace(tzinfo=None)

        valor = float(inf.findtext(".//vTPrest", default="0"))
        pagador = inf.findtext(".//rem/xNome", default="Cliente Diverso")
        
        # Extração de Filial (Emitente)
        xFant = inf.findtext(".//emit/xFant")
        xNome = inf.findtext(".//emit/xNome")
        filial = xFant if xFant else (xNome if xNome else "Matriz")

        # Data de Transmissão (Protocolo)
        prot = root.find(".//protCTe")
        data_transmissao = None
        if prot:
            infProt = prot.find("infProt")
            if infProt is not None:
                dh_recbto = infProt.findtext("dhRecbto")
                if dh_recbto:
                    dt_trans = pd.to_datetime(dh_recbto, errors="coerce")
                    if dt_trans is not None:
                        data_transmissao = dt_trans.replace(tzinfo=None)

        return {
            "Numero_CTe": numero,
            "Data_Emissao": data,
            "Data_Transmissao": data_transmissao,
            "Pagador": pagador,
            "Filial": filial,
            "Valor_Total_Frete": valor
        }
    except Exception:
        return None


# ------------------------------------------
# FUNÇÃO: BUSCAR DADOS (INCREMENTAL)
# ------------------------------------------
# Removido cache_data para gerenciar manualmente no session_state
# FUNÇÃO: BUSCAR DADOS (PAGINADO COM CURSOR)
# ------------------------------------------
def fetch_batch(token, subdomain, start_param):
    """
    Busca UM ou ALGUNS lotes de dados.
    start_param: pode ser {"since": "..."} ou {"start": "NEXT_ID"}
    Retorna: (lista_items, proximo_cursor_str ou None)
    """
    base_url = f"https://{subdomain}.eslcloud.com.br/api/ctes"
    headers = {"Authorization": f"Token {token}"}
    
    # Adiciona limite padrão
    params = start_param.copy()
    params["limit"] = 100 
    
    try:
        r = requests.get(base_url, headers=headers, params=params, timeout=15)
        if r.status_code != 200:
            return [], None
        payload = r.json()
    except:
        return [], None
        
    items = payload.get("data", [])
    next_id = payload.get("paging", {}).get("next_id")
    
    return items, next_id

# ------------------------------------------
# GERENCIADOR DE DADOS GLOBAL (Persiste no F5)
# ------------------------------------------
@st.cache_resource
class StatsManager:
    def __init__(self):
        self.cte_storage = {}
        self.last_days_back = 0
        self.last_sync_time = None
        
        # Estado de sincronização contínua
        self.resume_token = None # Se diferente de None, indica que tem mais páginas
        self.is_syncing = False # Flag visual
        self.current_params = {} # Armazena os parâmetros da última requisição para continuar

    def get_all(self):
        return list(self.cte_storage.values())
        
    def sync_step(self, token, subdomain, days_back, time_limit=2.0):
        """
        Executa passos de sincronização por no máximo `time_limit` segundos.
        Retorna (novos_items_count, continua_proxima_run?)
        """
        fuso_br = timezone(timedelta(hours=-3))
        agora = datetime.now(fuso_br)
        start_time = time.time()
        
        # 1. Detectar necessidade de Full Reload (Resetar cursor)
        if days_back > self.last_days_back:
            # User pediu mais dias, resetamos para buscar tudo desde o novo 'since'
            self.last_days_back = days_back
            self.resume_token = None
            
            since = (agora - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
            self.current_params = {"since": since}
            
        elif not self.cte_storage and not self.resume_token:
             # Cache vazio (primeiro load) e não estamos no meio de uma sync
            self.last_days_back = days_back
            self.resume_token = None
            since = (agora - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
            self.current_params = {"since": since}

        elif self.resume_token is None:
            # Incremental (só os últimos 7 dias)
            # Iniciamos um novo ciclo incremental se não houver um em andamento
            start_date = agora - timedelta(days=7)
            since = start_date.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
            self.current_params = {"since": since}
        
        # Se resume_token já tem next_id (continuação), usamos ele
        if self.resume_token:
            self.current_params = {"start": self.resume_token}
            
        count_new_session = 0
        has_more = False
        
        # Loop pequeno (Time Boxed)
        while True:
            # Verifica tempo
            if (time.time() - start_time) > time_limit:
                 has_more = True
                 break
            
            items, next_id = fetch_batch(token, subdomain, self.current_params)
            
            if not items:
                # Fim da linha para este batch
                if not next_id:
                    self.resume_token = None # Fim total
                    has_more = False
                    break
            
            # Processar Itens
            for item in items:
                try:
                    cte_data = item.get("cte", item)
                    item_id = cte_data.get("id") or item.get("id")
                    
                    if not item_id:
                        xml_c = cte_data.get("xml") or cte_data.get("content")
                        if xml_c:
                            parsed = parse_cte_xml(xml_c)
                            if parsed: item_id = parsed.get("Numero_CTe")
                    
                    if item_id:
                        if item_id not in self.cte_storage:
                            count_new_session += 1
                        self.cte_storage[item_id] = item
                    else:
                        # Hash fallback
                        xml_c = cte_data.get("xml") or cte_data.get("content")
                        if xml_c:
                            h = str(hash(xml_c))
                            if h not in self.cte_storage: count_new_session += 1
                            self.cte_storage[h] = item
                except:
                    pass
            
            # Preparar próxima página
            if next_id:
                self.resume_token = next_id
                self.current_params = {"start": next_id}
                has_more = True # Tem mais, mas vamos ver se dá tempo de pegar no proximo loop
            else:
                self.resume_token = None
                has_more = False
                break
        
        self.last_sync_time = datetime.now()
        self.is_syncing = has_more
        return count_new_session, has_more

@st.cache_resource
def get_manager():
    mgr = StatsManager()
    # Hot-fix: Se o objeto já existia no cache (criado antes da atualização do código),
    # ele pode não ter os novos atributos. Vamos injetá-los agora.
    if not hasattr(mgr, "is_syncing"): 
        mgr.is_syncing = False
    if not hasattr(mgr, "resume_token"): 
        mgr.resume_token = None
    if not hasattr(mgr, "current_params"): 
        mgr.current_params = {}
    return mgr


# ------------------------------------------
# DASHBOARD
# ------------------------------------------
st.title("🚛 Painel Financeiro em Tempo Real")

if CONNECT_API and TOKEN:
    mgr = get_manager()
    
    # 1. RECUPERA DADOS DO CACHE (Instantâneo)
    items = mgr.get_all()
    
    # Se cache vazio, avisa que vai demorar
    if not items:
        st.info("🚀 Iniciando carga inicial de dados... Isso pode levar alguns segundos.")
    
    last_sync_txt = mgr.last_sync_time.strftime('%H:%M:%S') if mgr.last_sync_time else "Nunca"
    st.caption(f"🕒 Última atualização do Cache: {last_sync_txt} (Auto-Refresh ativo)")
    
    # Botão de Reset GLOBAL
    if st.sidebar.button("🗑️ Resetar Tudo (Global)"):
        st.cache_resource.clear()
        st.rerun()

    # 2. RENDERIZA DASHBOARD COM O QUE TEM (Para não travar visualização)
    # (A lógica continua abaixo com a variável 'items')
    
    records = []
    for item in items:
        cte = item.get("cte", item)
        if cte.get("status") in ["canceled", "denied"]:
            continue
            
        xml = cte.get("xml") or cte.get("content")
        parsed = parse_cte_xml(xml)
        if parsed:
            parsed["Status_API"] = cte.get("status", "unknown")
            records.append(parsed)
            
    df = pd.DataFrame(records)

    # --- DIAGNÓSTICO DE STATUS (SIDEBAR) ---
    with st.sidebar.expander("📊 Diagnóstico de Status (Raw)", expanded=False):
        if not df.empty and "Status_API" in df.columns:
            status_counts = df["Status_API"].value_counts().reset_index()
            status_counts.columns = ["Status", "Qtd"]
            st.write("Total Carregado:", len(df))
            st.dataframe(status_counts, hide_index=True)
        else:
            st.warning("Nenhum dado processado (DataFrame vazio).")

else:
    df = pd.DataFrame()


if not df.empty:
    # DEFINIÇÃO DE DATAS (HOJE, ONTEM, MÊS PASSADO)
    fuso_brasil = timezone(timedelta(hours=-3))
    agora = datetime.now(fuso_brasil)
    
    hoje = agora.date()
    ontem = hoje - timedelta(days=1)
    
    # Datas para comparação (Mês Anterior)
    hoje_mes_passado = hoje - pd.DateOffset(months=1)
    ontem_mes_passado = ontem - pd.DateOffset(months=1)
    
    # Conversão para data simples para filtro
    hoje_mp_date = hoje_mes_passado.date()
    ontem_mp_date = ontem_mes_passado.date()

    df = pd.DataFrame(records)
    
    # --- SIMULAÇÃO DE CENÁRIOS (DEBUG) ---
    # Vamos calcular quanto daria se incluíssemos TUDO (cancelados, denegados, etc)
    # Re-processar lista crua sem filtros
    all_records = []
    for item in items:
        cte = item.get("cte", item)
        # SEM FILTRO DE STATUS
        xml = cte.get("xml") or cte.get("content")
        parsed = parse_cte_xml(xml)
        if parsed:
            # Adiciona o status ao objeto para agrupar
            parsed["Status"] = cte.get("status", "unknown")
            all_records.append(parsed)
    
    df_all = pd.DataFrame(all_records)
    
    if not df_all.empty:
        df_all["Data_Ref"] = df_all["Data_Emissao"].dt.date
        
        # Filtro Novembro (Hardcoded para teste rápido ou dinâmico)
        # User disse "Novembro", então vamos filtrar mês 11/2025 (ou ano atual)
        # Mas melhor mostrar o GERAL dos dados baixados (que é 60 dias)
        
        with st.sidebar.expander("🕵️‍♂️ Comparativo de Status (Simulação)", expanded=True):
            st.write("Se considerarmos **TODOS** os status:")
            
            # Agrupa por Status
            resumo = df_all.groupby("Status").agg(
                Qtd=("Numero_CTe", "count"),
                Valor=("Valor_Total_Frete", "sum")
            ).reset_index()
            
            st.dataframe(resumo.style.format({"Valor": "R$ {:,.2f}"}), hide_index=True)
            
            total_qtd = df_all["Numero_CTe"].count()
            total_val = df_all["Valor_Total_Frete"].sum()
            
            st.caption("Compare esses números com o do seu sistema. Se bater, é porque o sistema conta cancelados!")

    df["Data_Ref"] = df["Data_Emissao"].dt.date
    
    # Lógica de Não Transmitido:
    # 1. Identificar PENDENTES antes de filtrar o DataFrame principal (para o alerta)
    df_raw = df.copy()
    
    # Identificar "Em Processamento / Não Transmitido" (No Raw)
    df_raw["Nao_Transmitido"] = (
        (df_raw["Status_API"] != "authorized") | 
        (df_raw["Data_Transmissao"].isna())
    )
    # Ignorar cancelados no alerta de pendente se o user não quiser ver cancelados pendentes (geralmente não quer)
    df_pendentes = df_raw[
        (df_raw["Nao_Transmitido"] == True) & 
        (~df_raw["Status_API"].isin(["canceled", "denied"]))
    ]

    # --- FILTRO FINAL: APENAS AUTORIZADOS PARA O FINANCEIRO ---
    # Convertemos para minúsculo para garantir compatibilidade
    if "Status_API" in df.columns:
        df["Status_Normalizado"] = df["Status_API"].astype(str).str.lower().str.strip()
        df = df[df["Status_Normalizado"] == "authorized"]

    # --- DEDUPLICAÇÃO INTELIGENTE REMOVIDA TEMPORARIAMENTE ---
    # O filtro por número simples pode ter removido CT-es de Séries diferentes (Ex: Série 1 e 2 com mesmo número).
    # Vamos confiar no ID único da API que o StatsManager já gerencia.
    # if not df.empty:
    #    df = df.drop_duplicates(subset=["Numero_CTe"], keep="last")
    
    # Identificar Lag (Atraso na transmissão) em Minutos (Agora só nos autorizados)
    df["Lag_Minutos"] = (df["Data_Transmissao"] - df["Data_Emissao"]).dt.total_seconds() / 60.0
    df["Lag_Minutos"] = df["Lag_Minutos"].fillna(0) 

    # --- FILTROS DE DADOS ---
    df_hoje = df[df["Data_Ref"] == hoje]
    df_hoje_mp = df[df["Data_Ref"] == hoje_mp_date] 

    df_ontem = df[df["Data_Ref"] == ontem]
    df_ontem_mp = df[df["Data_Ref"] == ontem_mp_date] # Comparativo

    # --- MÉTRICAS E DELTAS ---
    def calc_delta(atual, anterior):
        if anterior == 0: return 0.0
        return ((atual - anterior) / anterior) * 100

    # KPI 1: HOJE
    val_hoje = df_hoje["Valor_Total_Frete"].sum()
    val_hoje_mp = df_hoje_mp["Valor_Total_Frete"].sum()
    delta_val_hoje = calc_delta(val_hoje, val_hoje_mp)
    
    qtd_hoje = len(df_hoje)
    qtd_hoje_mp = len(df_hoje_mp)
    delta_qtd_hoje = calc_delta(qtd_hoje, qtd_hoje_mp)

    val_ontem = df_ontem["Valor_Total_Frete"].sum()
    val_ontem_mp = df_ontem_mp["Valor_Total_Frete"].sum()
    delta_val_ontem = calc_delta(val_ontem, val_ontem_mp)

    # KPI 3: MÊS ATUAL (Vigente)
    mes_atual_start = hoje.replace(day=1)
    df_mes_atual = df[df["Data_Ref"] >= mes_atual_start]
    val_mes_atual = df_mes_atual["Valor_Total_Frete"].sum()
    
    # KPI 4: MÊS ANTERIOR (FECHADO)
    mes_passado_start = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    mes_passado_end = hoje.replace(day=1) - timedelta(days=1)
    
    df_mes_passado = df[
        (df["Data_Ref"] >= mes_passado_start) & 
        (df["Data_Ref"] <= mes_passado_end)
    ]
    val_mes_passado = df_mes_passado["Valor_Total_Frete"].sum()
    
    meses_pt = {1:"Janeiro", 2:"Fevereiro", 3:"Março", 4:"Abril", 5:"Maio", 6:"Junho", 
                7:"Julho", 8:"Agosto", 9:"Setembro", 10:"Outubro", 11:"Novembro", 12:"Dezembro"}
    nome_mes_passado = meses_pt.get(mes_passado_start.month, "Mês Anterior")
    nome_mes_atual = meses_pt.get(mes_atual_start.month, "Mês Atual")

    # KPI 5: ANO ATUAL (YTD)
    ano_atual = hoje.year
    df_ano = df[df["Data_Emissao"].dt.year == ano_atual]
    val_ano = df_ano["Valor_Total_Frete"].sum()

    # DISPLAY (5 COLUNAS)
    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric(
        label=f"Hoje ({hoje.strftime('%d/%m')})",
        value=f"R$ {val_hoje:,.2f}",
        delta=f"{delta_val_hoje:+.1f}%",
        delta_color="normal"
    )

    c2.metric(
        label=f"Ontem ({ontem.strftime('%d/%m')})",
        value=f"R$ {val_ontem:,.2f}",
        delta=f"{delta_val_ontem:+.1f}%",
        delta_color="normal"
    )
    
    c3.metric(
        label=f"{nome_mes_atual} (Em Curso)",
        value=f"R$ {val_mes_atual:,.2f}",
        delta=f"{len(df_mes_atual)} CT-es",
        delta_color="off"
    )
    
    c4.metric(
        label=f"{nome_mes_passado} (Fechado)",
        value=f"R$ {val_mes_passado:,.2f}",
        delta=f"{len(df_mes_passado)} CT-es",
        delta_color="off"
    )

    # --- AUDITORIA DE DIVERGÊNCIA (MÊS PASSADO) ---
    # REMOVIDO: Como agora filtramos TUDO para authorized, não deve haver divergência.
    # Se houver, é porque a API retornou authorized mas o sistema diz outra coisa.


    c5.metric(
        label=f"Ano {ano_atual}",
        value=f"R$ {val_ano:,.2f}",
        delta=f"{len(df_ano)} CT-es",
        delta_color="off"
    )

    # --- KPI EXTRA: NÃO TRANSMITIDOS ---
    # --- KPI EXTRA: NÃO TRANSMITIDOS ---
    st.divider()
    
    # Usamos o df_pendentes calculado ANTES do filtro strict
    qtd_pendente = len(df_pendentes)
    val_pendente = df_pendentes["Valor_Total_Frete"].sum()
    
    if qtd_pendente > 0:
        st.warning(f"⚠️ **Atenção:** Existem **{qtd_pendente} CT-es** detectados como **Não Transmitidos** (R$ {val_pendente:,.2f}).")
        with st.expander("Ver CT-es Não Transmitidos"):
            st.dataframe(
                df_pendentes[["Numero_CTe", "Data_Emissao", "Status_API", "Valor_Total_Frete"]]
                .style.format({"Valor_Total_Frete": "R$ {:,.2f}", "Data_Emissao": "{:%d/%m %H:%M}"}),
                use_container_width=True
            )
    
    st.divider()

    # ------------------------------------------
    # GRÁFICO COMPARATIVO MELHORADO
    # ------------------------------------------
    # Range strings para o título
    start_atual = hoje.replace(day=1)
    end_atual = hoje
    range_atual_str = f"{start_atual.strftime('%d/%m')} a {end_atual.strftime('%d/%m')}"
    
    start_anterior = mes_passado_start
    end_anterior = mes_passado_end
    range_anterior_str = f"{start_anterior.strftime('%d/%m')} a {end_anterior.strftime('%d/%m')}"
    
    st.subheader(f"📊 Comparativo Dia a Dia ({nome_mes_atual} vs {nome_mes_passado})")

    # Preparar Dados (Calendário Civil)
    df_atual = df[df["Data_Ref"] >= start_atual].copy()
    
    df_anterior = df[
        (df["Data_Ref"] >= start_anterior) & 
        (df["Data_Ref"] <= end_anterior)
    ].copy()

    df_atual["Dia_Mes"] = df_atual["Data_Emissao"].dt.day
    df_anterior["Dia_Mes"] = df_anterior["Data_Emissao"].dt.day

    # Agrupar
    grp_atual = df_atual.groupby("Dia_Mes")["Valor_Total_Frete"].sum().reset_index()
    grp_atual["Periodo"] = "Atual"
    
    grp_anterior = df_anterior.groupby("Dia_Mes")["Valor_Total_Frete"].sum().reset_index()
    grp_anterior["Periodo"] = "Anterior"

    # Criação do Gráfico de Barras
    fig = go.Figure()

    # Barra Mês Anterior
    fig.add_trace(go.Bar(
        x=grp_anterior["Dia_Mes"], 
        y=grp_anterior["Valor_Total_Frete"],
        name=f"Anterior ({nome_mes_passado})",
        marker_color='#9ca3af', # Cinza
        hovertemplate=f"Dia %{{x}} ({nome_mes_passado})<br><b>R$ %{{y:,.2f}}</b><extra></extra>",
        text=[f"{v/1000:.1f}k" for v in grp_anterior["Valor_Total_Frete"]],
        textposition="outside"
    ))

    # Barra Mês Atual
    fig.add_trace(go.Bar(
        x=grp_atual["Dia_Mes"], 
        y=grp_atual["Valor_Total_Frete"],
        name=f"Atual ({nome_mes_atual})",
        marker_color='#0ea5e9', # Azul
        hovertemplate=f"Dia %{{x}} ({nome_mes_atual})<br><b>R$ %{{y:,.2f}}</b><extra></extra>",
        text=[f"{v/1000:.1f}k" for v in grp_atual["Valor_Total_Frete"]],
        textposition="outside"
    ))

    fig.update_layout(
        barmode='group',
        xaxis_title="Dia do Mês",
        yaxis_title="Faturamento (R$)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=30, b=20),
        xaxis=dict(tickmode='linear', tick0=1, dtick=1) # Mostrar todos os dias
    )

    st.plotly_chart(fig, use_container_width=True)



    # ------------------------------------------
    # DETALHES FINAIS
    # ------------------------------------------
    # ------------------------------------------
    # RANKING DE FILIAIS E CLIENTES
    # ------------------------------------------
    # ------------------------------------------
    # RANKING DE FILIAIS E CLIENTES
    # ------------------------------------------
    try:
        # Debug Temporário
        # st.write("Colunas disponíveis:", df.columns.tolist())
        
        if "Filial" not in df.columns:
            st.error("Erro: Coluna 'Filial' não encontrada no DataFrame.")
            # Fallback para criar a coluna se não existir
            df["Filial"] = "Não Identificada"
            df_atual["Filial"] = "Não Identificada"

        c_filial, c_pie = st.columns(2)
        
        with c_filial:
            st.subheader("🏆 Filiais (Faturamento Mês)")
            ranking_filial = df_atual.groupby("Filial")["Valor_Total_Frete"].sum().reset_index().sort_values("Valor_Total_Frete", ascending=True)
            
            fig_f = px.bar(
                ranking_filial,
                x="Valor_Total_Frete",
                y="Filial",
                orientation='h',
                text_auto='.2s',
                color_discrete_sequence=['#0ea5e9']
            )
            fig_f.update_layout(
                xaxis_title="Faturamento (R$)",
                yaxis_title=None,
                margin=dict(l=20, r=20, t=10, b=20)
            )
            st.plotly_chart(fig_f, use_container_width=True)

        with c_pie:
            st.subheader("Top Clientes")
            top_cli = df.groupby("Pagador")["Valor_Total_Frete"].sum().nlargest(5).reset_index()
            fig_p = px.pie(
                top_cli, 
                values="Valor_Total_Frete", 
                names="Pagador", 
                hole=0.5,
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            fig_p.update_traces(
                textposition='outside', 
                textinfo='percent+label',
                textfont=dict(size=14, family="Arial Black"),
                hovertemplate = "%{label}: R$ %{value:,.2f} (%{percent})"
            )
            fig_p.update_layout(
                showlegend=False,
                margin=dict(t=40, b=40, l=40, r=40)
            )
            st.plotly_chart(fig_p, use_container_width=True)
            
    except Exception as e:
        st.error(f"Ocorreu um erro ao gerar os gráficos de Filial/Cliente: {e}")

    st.divider()

    # ------------------------------------------
    # PREVISÃO DE FATURAMENTO (IA Mensal)
    # ------------------------------------------
    st.subheader("🔮 Estimativa de Faturamento (Mês Atual)")
    
    # Imports
    import calendar
    import numpy as np
    
    # 1. Preparar dados históricos
    start_hist = hoje - timedelta(days=60) # Pega pelo menos 2 meses para ver tendencia recente
    df_hist = df[
        (df["Data_Ref"] >= start_hist) & 
        (df["Data_Ref"] <= hoje)
    ].copy()

    # Debug de condições
    has_data = len(df_hist) > 10
    
    # Permitir projeção mesmo que mês atual seja zero (início de mês), 
    # desde que haja histórico suficiente para traçar a tendência.
    if has_data:
        # Agrupar por dia
        df_hist["Data_Ref"] = pd.to_datetime(df_hist["Data_Ref"])
        daily = df_hist.groupby("Data_Ref")["Valor_Total_Frete"].sum().reset_index()
        daily = daily.sort_values("Data_Ref")
        
        # Lógica Ajustada: Média baseada no histórico do Mês Passado
        # (Substitui regressão linear por projeção baseada na média diária do mês anterior)
        
        # Dias no mês passado
        total_dias_passado = (mes_passado_end - mes_passado_start).days + 1
        
        if val_mes_passado > 0 and total_dias_passado > 0:
            media_diaria_passada = val_mes_passado / total_dias_passado
            
            # Dias restantes no mês atual
            last_day_month = calendar.monthrange(hoje.year, hoje.month)[1]
            fim_do_mes = hoje.replace(day=last_day_month)
            dias_restantes = (fim_do_mes - hoje).days
            
            if dias_restantes > 0:
                previsao_restante = media_diaria_passada * dias_restantes
                previsao_total_mes = val_mes_atual + previsao_restante
            else:
                previsao_total_mes = val_mes_atual
        else:
            # Fallback se não tiver histórico suficiente: usa média atual
            if hoje.day > 0:
                media_atual = val_mes_atual / hoje.day
                last_day_month = calendar.monthrange(hoje.year, hoje.month)[1]
                previsao_total_mes = media_atual * last_day_month
            else:
                previsao_total_mes = val_mes_atual
                
            # Comparativo com mês passado
            delta_forecast = 0
            if val_mes_passado > 0:
                delta_forecast = ((previsao_total_mes - val_mes_passado) / val_mes_passado) * 100
                
            c_proj_1, c_proj_2 = st.columns([3, 1])
            
            with c_proj_1:
                 st.info(f"Com base no ritmo atual, a estimativa para fechar **{nome_mes_atual}** é de aproximadamente **R$ {previsao_total_mes:,.2f}**.")
                 st.progress(min(1.0, val_mes_atual / previsao_total_mes) if previsao_total_mes > 0 else 0)
                 st.caption(f"Já realizamos R$ {val_mes_atual:,.2f} ({val_mes_atual/previsao_total_mes:.1%} da previsão).")
                 
            with c_proj_2:
                st.metric(
                    label="Projeção vs Mês Anterior",
                    value=f"R$ {previsao_total_mes:,.2f}",
                    delta=f"{delta_forecast:+.1f}%",
                    delta_color="normal"
                )

            
    else:
        st.warning(f"Projeção indisponível no momento. (Dados Recentes: {len(df_hist)}, Faturamento Mês: {val_mes_atual:.2f})")
        st.caption("A IA precisa de pelo menos 5 dias de histórico recente e movimentação no mês atual para projetar.")

    st.divider()

    # ------------------------------------------
    # TABELA FINAL (FULL WIDTH)
    # ------------------------------------------
    st.subheader("📝 Últimas Emissões (Recentes)")
    st.dataframe(
        df.sort_values("Data_Emissao", ascending=False)
        .head(100)
        [["Numero_CTe", "Data_Emissao", "Pagador", "Valor_Total_Frete", "Filial"]]
        .style.format({"Valor_Total_Frete": "R$ {:,.2f}", "Data_Emissao": "{:%d/%m %H:%M}"}),
        use_container_width=True,
        height=400
    )


# ------------------------------------------
# MENSAGEM DE ESPERA (CASO DF VAZIO)
# ------------------------------------------
if df.empty and CONNECT_API and TOKEN:
     st.info("⏳ Aguardando sincronização de dados... O painel será atualizado automaticamente.")

# ------------------------------------------
# SINCRONIZAÇÃO EM BACKGROUND (RESUMABLE / STREAMING)
# ------------------------------------------
if CONNECT_API and TOKEN:
    status_placeholder = st.sidebar.empty()
    
    # Se estivermos no meio de uma sincronização (temos token de resumo)
    # ou se for a primeira vez, mostramos status ativo.
    if mgr.is_syncing or mgr.resume_token:
        status_placeholder.text("🔄 Baixando dados...")
    else:
        status_placeholder.text("✅ Tudo atualizado.")
    
    try:
        # Executa um passo de sincronização (max 5 segundos para reduzir flash)
        qtd_novos, has_more = mgr.sync_step(TOKEN, SUBDOMAIN, DAYS_BACK, time_limit=5.0)
        
        if qtd_novos > 0:
            status_placeholder.success(f"Recebidos +{qtd_novos} itens!")
            # Delay mínimo para o usuário ver que chegou coisa nova antes do refresh
            time.sleep(0.1)
            st.rerun()
            
        elif has_more:
            # Não trouxe novos *neste* passo, mas tem mais páginas.
            # Rerun imediato para buscar o próximo lote sem travar a UI.
            st.rerun()
            
        else:
            # Terminou tudo.
            if mgr.is_syncing: 
                # Terminamos um ciclo grande. Limpamos o status visual.
                status_placeholder.empty()
                mgr.is_syncing = False
            else:
                # Check silencioso de rotina (já estava tudo ok)
                status_placeholder.empty()
            
    except Exception as e:
        status_placeholder.error(f"Erro sync: {e}")
        time.sleep(5) # Backoff em caso de erro grave
            
    except Exception as e:
        status_placeholder.error(f"Erro sync: {e}")
        time.sleep(5) # Backoff em caso de erro grave
