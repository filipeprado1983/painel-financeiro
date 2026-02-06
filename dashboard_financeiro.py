import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import requests
import xml.etree.ElementTree as ET
import re
import time
from streamlit_autorefresh import st_autorefresh

# ------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# ------------------------------------------
st.set_page_config(
    page_title="Dashboard Financeiro - Transportes",
    page_icon="🚛",
    layout="wide"
)

# AUTO REFRESH: 15 minutos (900000ms)
st_autorefresh(interval=15 * 60 * 1000, limit=None, key="data_refresh")

# ------------------------------------------
# CSS
# ------------------------------------------
st.markdown("""
<style>
/* Main Background & Fonts */
.stApp {
    background-color: #0f172a; /* Slate 900 */
    font-family: 'Inter', sans-serif;
}

/* Card do KPI */
div[data-testid="metric-container"] {
    background-color: #1e293b; /* Slate 800 */
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    border: 1px solid #334155; /* Slate 700 */
    transition: transform 0.2s ease;
}

div[data-testid="metric-container"]:hover {
    transform: translateY(-2px);
    border-color: #3b82f6; /* Blue 500 */
}

/* Texto pequeno (título) */
div[data-testid="metric-container"] label {
    color: #94a3b8 !important; /* Slate 400 */
    font-size: 0.875rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* Texto grande (valor) */
div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
    color: #f8fafc !important; /* Slate 50 */
    font-size: 1.875rem;
    font-weight: 700;
}

/* Delta (Comparação) */
div[data-testid="metric-container"] div[data-testid="stMetricDelta"] {
    font-weight: 600;
    font-size: 0.875rem;
}

/* Adjust Streamlit Elements */
.stDataFrame {
    border: 1px solid #334155;
    border-radius: 8px;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------
# SIDEBAR
# ------------------------------------------
st.sidebar.header("Configuração")

if st.sidebar.button("🔄 Forçar Atualização Agora"):
    st.cache_data.clear()
    st.rerun()

# --- PERSISTÊNCIA DE TOKEN VIA URL ---
# Tenta pegar da URL primeiro
query_params = st.query_params
url_token = query_params.get("token", "7f-z3i1jYgura6oQBDKdeDxu2jBMqwAH2jFRjbGd2JJz9CBUWENQYA")
url_subdomain = query_params.get("subdomain", "trf")

SUBDOMAIN = st.sidebar.text_input("Subdomínio", value=url_subdomain)
TOKEN = st.sidebar.text_input("Token", type="password", value=url_token)
DAYS_BACK = st.sidebar.slider("Buscar últimos (dias)", 30, 180, 60)

# Atualiza a URL se os valores mudarem na sidebar (para persistir no F5)
if TOKEN and TOKEN != url_token:
    st.query_params["token"] = TOKEN
if SUBDOMAIN and SUBDOMAIN != url_subdomain:
    st.query_params["subdomain"] = SUBDOMAIN

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
        
        # Extração da Filial (Emitente)
        emit = inf.find("emit")
        filial = "Matriz"
        if emit is not None:
            filial = emit.findtext("xFant")
            if not filial:
                filial = emit.findtext("xNome")

        # Extração de Peso (qCarga) - Lógica de Seleção (Busca Global infQ)
        peso_final = 0.0
        pesos_encontrados = {}
        
        # Tenta buscar infQ em qualquer lugar do XML (ignora hierarquia infCarga)
        all_inf_qs = root.findall(".//infQ")
        
        if all_inf_qs:
            # print(f"[DEBUG] CT-e {numero}: Encontrados {len(all_inf_qs)} tags infQ")
            for inf_q in all_inf_qs:
                c_unid = inf_q.findtext("cUnid")
                tp_med = inf_q.findtext("tpMed", default="").upper()
                q_carga_txt = inf_q.findtext("qCarga", default="0").replace(",", ".")

                try:
                    val = float(q_carga_txt)
                except:
                    val = 0.0
                
                # Conversão para KG
                if c_unid == "02": # TON
                    val *= 1000
                
                # Armazena por tipo
                key = tp_med if tp_med else f"UNID_{c_unid}"
                pesos_encontrados[key] = val
            
            # Prioridade de Seleção
            prioridades = ["PESO BRUTO", "P.BRUTO", "PESO REAL", "P.REAL", "PESO", "PESO DECLARADO"]
            
            for p in prioridades:
                for chave_encontrada in pesos_encontrados:
                    if p in chave_encontrada:
                        peso_final = pesos_encontrados[chave_encontrada]
                        break
                if peso_final > 0:
                    break
            
            # Fallback 1: Unid 01 (KG) mas sem nome de PESO CUBADO
            if peso_final == 0:
                for chave, valor in pesos_encontrados.items():
                    if "CUB" not in chave and "VOL" not in chave and "M3" not in chave:
                        if valor > peso_final:
                            peso_final = valor
            
            # Fallback 2: Maior valor
            if peso_final == 0 and pesos_encontrados:
                peso_final = max(pesos_encontrados.values())

        else:
             print(f"[DEBUG] CT-e {numero}: Nenhuma tag infQ encontrada no XML inteiro")

        return {
            "Numero_CTe": numero,
            "Data_Emissao": data,
            "Pagador": pagador,
            "Valor_Total_Frete": valor,
            "Filial": filial,
            "Peso_Kg": peso_final
        }
    except Exception as e:
        print(f"Erro no parse do XML: {e}")
        return None


# ------------------------------------------
# FUNÇÃO: BUSCAR DADOS (CACHE 30 MIN)
# ------------------------------------------
# ------------------------------------------
# FUNÇÃO: BUSCAR DADOS DA API (SEM CACHE DE SESSION)
# ------------------------------------------
def fetch_ctes_from_api(token, subdomain, since_dt):
    base_url = f"https://{subdomain}.eslcloud.com.br/api/ctes"
    headers = {"Authorization": f"Token {token}"}
    
    since = since_dt.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
    params = {"since": since, "limit": 100}
    
    all_items = []
    page = 1
    MAX_PAGES = 500
    
    status_box = st.empty()
    
    while page <= MAX_PAGES:
        if page > 1:
            time.sleep(0.5) # Reduzido para agilizar
            
        status_box.text(f"📥 Baixando novidades... Página {page} ({len(all_items)} novos)")
        
        try:
            r = requests.get(base_url, headers=headers, params=params, timeout=20)
        except Exception:
            break
            
        if r.status_code != 200:
            break
            
        try:
            payload = r.json()
        except Exception:
            break
            
        items = payload.get("data", [])
        if not items:
            break
            
        all_items.extend(items)
        
        next_id = payload.get("paging", {}).get("next_id")
        if not next_id:
            break
            
        # Paginacao via ID
        params = {"start": next_id, "limit": 100}
        page += 1
        
    status_box.empty()
    return all_items

# ------------------------------------------
# GERENCIAMENTO DE CACHE LOCAL (PARQUET)
# ------------------------------------------
CACHE_FILE = "ctes_store.parquet"
import os

def load_local_data():
    if os.path.exists(CACHE_FILE):
        try:
            return pd.read_parquet(CACHE_FILE)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_local_data(df):
    if not df.empty:
        # Garante que timestamps sejam salvos corretamente
        df.to_parquet(CACHE_FILE, index=False)

# ------------------------------------------
# DASHBOARD
# ------------------------------------------
st.title("🚛 Painel Financeiro em Tempo Real")

# Botão de Reset
if st.sidebar.button("⚠️ Forçar Ressincronização"):
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    st.cache_data.clear()
    st.rerun()

if CONNECT_API and TOKEN:
    # 1. Carregar Cache Existente
    df_cache = load_local_data()
    
    # 2. Definir Ponto de Partida
    if not df_cache.empty and "Data_Emissao" in df_cache.columns:
        # Pega a maior data registrada no cache
        last_date = df_cache["Data_Emissao"].max()
        # Recua 1 hora por segurança (evitar delay de sincronia)
        since_fetch = last_date - timedelta(hours=1)
        # Se o usuário pediu MAIS dias do que temos no cache, forçamos o slider
        # Mas para "Incremental", geralmente respeitamos o cache e só pegamos o novo.
        # Se quiser forçar histórico, usa o botão de reset.
        mode_str = f"Incremental (desde {last_date.strftime('%d/%m %H:%M')})"
    else:
        since_fetch = since_dt
        mode_str = f"Carga Inicial (últimos {DAYS_BACK} dias)"
        
    st.sidebar.text(f"Modo: {mode_str}")
    
    # 3. Buscar Novidades
    new_items = fetch_ctes_from_api(TOKEN, SUBDOMAIN, since_fetch)
    
    # 4. Processar Novos Itens
    new_records = []
    for item in new_items:
        cte = item.get("cte", item)
        if cte.get("status") in ["canceled", "denied"]:
            continue
        xml = cte.get("xml") or cte.get("content")
        parsed = parse_cte_xml(xml)
        if parsed:
            new_records.append(parsed)
            
    df_new = pd.DataFrame(new_records)
    
    # 5. Merge e Deduplicação
    if not df_new.empty:
        if not df_cache.empty:
            # Concatena
            df_combined = pd.concat([df_cache, df_new], ignore_index=True)
            # Remove duplicados pelo Numero/Chave (aqui usando Numero_CTe + Pagador/Data para garantir, ou só id se tivesse)
            # Vamos confiar no Numero_CTe como chave principal 'visível', ou melhor, remover duplicatas exatas
            df_final = df_combined.drop_duplicates(subset=["Numero_CTe", "Valor_Total_Frete"], keep="last")
        else:
            df_final = df_new
            
        # Salva o novo estado
        save_local_data(df_final)
        df = df_final
    else:
        df = df_cache

else:
    df = pd.DataFrame()


if not df.empty:
    # DEFINIÇÃO DE DATAS (HOJE, ONTEM, MÊS PASSADO)
    fuso_brasil = timezone(timedelta(hours=-3))
    agora = datetime.now(fuso_brasil)
    
    hoje = agora.date()
    ontem = hoje - timedelta(days=1)
    anteontem = hoje - timedelta(days=2)
    
    # Datas para comparação (Mês Anterior)
    hoje_mes_passado = hoje - pd.DateOffset(months=1)
    
    # Conversão para data simples para filtro
    hoje_mp_date = hoje_mes_passado.date()

    df["Data_Ref"] = df["Data_Emissao"].dt.date
    
    # Garantir colunas novas
    if "Filial" not in df.columns: df["Filial"] = "Não Identificado"
    else: df["Filial"] = df["Filial"].fillna("Não Identificado")
        
    if "Peso_Kg" not in df.columns: df["Peso_Kg"] = 0.0
    else: df["Peso_Kg"] = df["Peso_Kg"].fillna(0.0)

    # --- FILTROS DE DADOS ---
    df_hoje = df[df["Data_Ref"] == hoje]
    df_ontem = df[df["Data_Ref"] == ontem]
    df_anteontem = df[df["Data_Ref"] == anteontem]

    # Comparativo Mês Anterior (para KPI fechado)
    df_hoje_mp = df[df["Data_Ref"] == hoje_mp_date] 

    # --- MÉTRICAS E DELTAS ---
    def calc_delta(atual, anterior):
        if anterior == 0: return 0.0
        return ((atual - anterior) / anterior) * 100

    # KPI 1: HOJE (Comparado com ONTEM)
    val_hoje = df_hoje["Valor_Total_Frete"].sum()
    val_ontem = df_ontem["Valor_Total_Frete"].sum()
    
    # Logica de Delta: "Hoje vs Ontem"
    # Se ontem foi 0, o delta seria infinito, retornamos 0 ou 100? calc_delta retorna 0.
    delta_val_hoje = calc_delta(val_hoje, val_ontem)
    
    qtd_hoje = len(df_hoje)
    
    # KPI 2: ONTEM (Comparado com ANTEONTEM)
    val_anteontem = df_anteontem["Valor_Total_Frete"].sum()
    delta_val_ontem = calc_delta(val_ontem, val_anteontem)

    # KPI Peso e Média/Kg (HOJE)
    peso_hoje = df_hoje["Peso_Kg"].sum()
    media_kg_hoje = (val_hoje / peso_hoje) if peso_hoje > 0 else 0.0
    
    # KPI Peso e Média/Kg (ONTEM)
    peso_ontem = df_ontem["Peso_Kg"].sum()
    media_kg_ontem = (val_ontem / peso_ontem) if peso_ontem > 0 else 0.0

    # KPI 3: MÊS ANTERIOR (FECHADO)
    mes_passado_start = (hoje.replace(day=1) - timedelta(days=1)).replace(day=1)
    mes_passado_end = hoje.replace(day=1) - timedelta(days=1)
    
    df_mes_passado = df[
        (df["Data_Ref"] >= mes_passado_start) & 
        (df["Data_Ref"] <= mes_passado_end)
    ]
    val_mes_passado = df_mes_passado["Valor_Total_Frete"].sum()
    nome_mes_passado = mes_passado_start.strftime("%B/%Y") # Pode precisar de ajuste de locale ou hardcode

    # KPI 4: ANO ATUAL (YTD)
    ano_atual = hoje.year
    df_ano = df[df["Data_Emissao"].dt.year == ano_atual]
    val_ano = df_ano["Valor_Total_Frete"].sum()

    # DISPLAY ROW 1: FINANCEIRO
    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label=f"Hoje ({hoje.strftime('%d/%m')})",
        value=f"R$ {val_hoje:,.2f}",
        delta=f"{delta_val_hoje:+.1f}% vs Ontem",
        delta_color="normal"
    )

    c2.metric(
        label=f"Ontem ({ontem.strftime('%d/%m')})",
        value=f"R$ {val_ontem:,.2f}",
        delta=f"{delta_val_ontem:+.1f}% vs Anteontem",
        delta_color="normal"
    )
    
    # Ex: Novembro
    meses_pt = {1:"Janeiro", 2:"Fevereiro", 3:"Março", 4:"Abril", 5:"Maio", 6:"Junho", 
                7:"Julho", 8:"Agosto", 9:"Setembro", 10:"Outubro", 11:"Novembro", 12:"Dezembro"}
    nome_mes = meses_pt.get(mes_passado_start.month, "Mês Anterior")
    
    c3.metric(
        label=f"{nome_mes} (Fechado)",
        value=f"R$ {val_mes_passado:,.2f}",
        delta=f"{len(df_mes_passado)} CT-es",
        delta_color="off"
    )

    c4.metric(
        label=f"Ano {ano_atual} (Carregado)",
        value=f"R$ {val_ano:,.2f}",
        delta=f"{len(df_ano)} CT-es",
        delta_color="off"
    )

    # DISPLAY ROW 2: OPERACIONAL (PESO E MÉDIA)
    try:
        st.markdown("### ⚖️ Indicadores Operacionais (Hoje)")
        col_op1, col_op2, col_op3, col_op4 = st.columns(4)
        
        # Variação do Peso em relação a ontem (opcional, mas bom ter)
        delta_peso_hoje = calc_delta(peso_hoje, peso_ontem)

        col_op1.metric(
            label="Peso Total (Hoje)",
            value=f"{peso_hoje/1000:,.1f} ton",
            delta=f"{delta_peso_hoje:+.1f}% vs Ontem",
            delta_color="normal"
        )

        col_op2.metric(
            label="Média Frete / Kg (Hoje)",
            value=f"R$ {media_kg_hoje:.2f} /kg",
            help="Cálculo: Total Frete Hoje / Peso Total Hoje",
            delta=None
        )
    except Exception:
        st.error("Erro ao calcular indicadores operacionais")

    st.divider()

    # ------------------------------------------
    # GRÁFICO COMPARATIVO MELHORADO
    # ------------------------------------------
    # Range strings para o título
    mes_atual_start = hoje.replace(day=1)
    nome_mes_atual = meses_pt.get(mes_atual_start.month, mes_atual_start.strftime("%B"))
    range_atual_str = f"{nome_mes_atual}/{mes_atual_start.year}"
    
    # Mês Anterior (Completo)
    mes_anterior_end = mes_atual_start - timedelta(days=1)
    mes_anterior_start = mes_anterior_end.replace(day=1)
    nome_mes_anterior = meses_pt.get(mes_anterior_start.month, mes_anterior_start.strftime("%B"))
    range_anterior_str = f"{nome_mes_anterior}/{mes_anterior_start.year}"
    
    st.subheader(f"📊 Comparativo Dia a Dia ({range_atual_str} vs {range_anterior_str})")

    # Preparar Dados
    df_atual = df[df["Data_Ref"] >= mes_atual_start].copy()
    df_anterior = df[
        (df["Data_Ref"] >= mes_anterior_start) & 
        (df["Data_Ref"] <= mes_anterior_end)
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
        name=f"Anterior ({range_anterior_str})",
        marker_color='#64748b', # Slate 500
        hovertemplate=f"Dia %{{x}} ({range_anterior_str})<br><b>R$ %{{y:,.2f}}</b><extra></extra>",
        text=[f"{v/1000:.1f}k" for v in grp_anterior["Valor_Total_Frete"]],
        textposition="outside"
    ))

    # Barra Mês Atual
    fig.add_trace(go.Bar(
        x=grp_atual["Dia_Mes"], 
        y=grp_atual["Valor_Total_Frete"],
        name=f"Atual ({range_atual_str})",
        marker_color='#3b82f6', # Blue 500
        hovertemplate=f"Dia %{{x}} ({range_atual_str})<br><b>R$ %{{y:,.2f}}</b><extra></extra>",
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
    # FATURAMENTO POR FILIAL (MES ATUAL)
    # ------------------------------------------
    st.subheader(f"🏢 Faturamento por Filial ({range_atual_str})")
    
    # Agrupar por filial no mês atual
    grp_filial = df_atual.groupby("Filial")["Valor_Total_Frete"].sum().reset_index()
    grp_filial = grp_filial.sort_values("Valor_Total_Frete", ascending=True)

    if not grp_filial.empty:
        fig_filial = px.bar(
            grp_filial,
            x="Valor_Total_Frete",
            y="Filial",
            orientation='h',
            text_auto='.2s',
            color_discrete_sequence=['#10b981'] # Emerald 500
        )
        fig_filial.update_traces(
            textposition="outside",
            hovertemplate="Filial: %{y}<br>Faturamento: R$ %{x:,.2f}<extra></extra>"
        )
        fig_filial.update_layout(
            xaxis_title="Faturamento (R$)", 
            yaxis_title="",
            margin=dict(l=20, r=20, t=20, b=20)
        )
        st.plotly_chart(fig_filial, use_container_width=True)
    else:
        st.info("Sem dados de filial para o período atual.")

    # ------------------------------------------
    # EXPORTAÇÃO
    # ------------------------------------------
    st.download_button(
        label="📥 Baixar Dados (CSV)",
        data=df.to_csv(index=False).encode('utf-8'),
        file_name=f"financeiro_transp_{hoje}.csv",
        mime="text/csv",
        help="Baixar todos os dados carregados em formato CSV"
    )

    # ------------------------------------------
    # DETALHES FINAIS
    # ------------------------------------------
    c_pie, c_table = st.columns([1, 2])

    with c_pie:
        st.subheader("Top Clientes")
        top_cli = df.groupby("Pagador")["Valor_Total_Frete"].sum().nlargest(5).reset_index()
        fig_p = px.pie(
            top_cli, 
            values="Valor_Total_Frete", 
            names="Pagador", 
            hole=0.6,
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

    with c_table:
        st.subheader("Últimas Emissões (Recentes)")
        st.dataframe(
            df.sort_values("Data_Emissao", ascending=False)
            .head(100)
            [["Numero_CTe", "Data_Emissao", "Pagador", "Valor_Total_Frete"]]
            .style.format({"Valor_Total_Frete": "R$ {:,.2f}", "Data_Emissao": "{:%d/%m %H:%M}"}),
            use_container_width=True,
            height=400
        )

else:
    st.info("Aguardando dados... Verifique o token e clique em conectar.")
