"""
app.py
Dashboard LeucemiaPR — análise interativa de internações por leucemia no Paraná (2010-2026).
Execute: streamlit run app.py
"""

from pathlib import Path
import os

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="LeucemiaPR - Análise de Leucemia no Paraná",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Caminhos absolutos ────────────────────────────────────────────────────────
try:
    PROJECT_ROOT = Path(__file__).parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd())

GOLD_DIR = PROJECT_ROOT / "datalake" / "gold"
GOLD_EVOLUCAO  = str(GOLD_DIR / "gold_evolucao_anual.parquet")
GOLD_MORTAL    = str(GOLD_DIR / "gold_mortalidade_faixa.parquet")
GOLD_MUNICIPIO = str(GOLD_DIR / "gold_municipios.parquet")

# ── Importação do mapeamento completo de municípios ──────────────────────────
from municipios_pr import MUNICIPIOS_PR




# ── Carregamento com cache ────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def carregar(path: str) -> pd.DataFrame:
    """Carrega parquet via DuckDB com cache de 1 hora."""
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{path}')"
    ).df()


def verificar_gold() -> bool:
    """Verifica se todos os arquivos Gold existem."""
    return all(Path(p).exists() for p in [GOLD_EVOLUCAO, GOLD_MORTAL, GOLD_MUNICIPIO])


# ── Header principal ──────────────────────────────────────────────────────────
st.title("🩺 LeucemiaPR")
st.markdown(
    """
    ### Análise de Internações por Leucemia no Paraná (2010–2025)
    
    Dashboard interativo com dados do **SIH/SUS via DATASUS** · CIDs **C91–C95** (leucemias)
    """
)
st.divider()

# ── Verificação dos dados ─────────────────────────────────────────────────────
if not verificar_gold():
    st.error(
        "**Tabelas Gold não encontradas!**\n\n"
        "Execute o pipeline primeiro:\n"
        "```bash\n"
        "python main.py\n"
        "```"
    )
    st.stop()

# ── Carregamento dos dados ────────────────────────────────────────────────────
with st.spinner("Carregando dados..."):
    df_evolucao  = carregar(GOLD_EVOLUCAO)
    df_mortal    = carregar(GOLD_MORTAL)
    df_municipio = carregar(GOLD_MUNICIPIO)

# ── Filtrar dados de 2026 (dados incompletos) ─────────────────────────────────
df_evolucao = df_evolucao[df_evolucao["ano"] < 2026]
df_mortal = df_mortal[df_mortal["ano"] < 2026]
df_municipio = df_municipio[df_municipio["ano"] < 2026]

# ── Sidebar com filtros ───────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Filtros")
    
    # Filtro de ano - removendo valores nulos para evitar erro NA ambiguous
    anos_disponiveis = sorted(df_evolucao["ano"].dropna().unique())
    ano_min, ano_max = st.select_slider(
        "Período de análise",
        options=anos_disponiveis,
        value=(min(anos_disponiveis), max(anos_disponiveis))
    )
    
    # Filtro de tipo de leucemia - removendo valores nulos também
    tipos = sorted(df_evolucao["tipo_leucemia"].dropna().unique())
    tipos_selecionados = st.multiselect(
        "Tipos de leucemia",
        options=tipos,
        default=tipos,
        help="Selecione os tipos para incluir na análise"
    )
    
    # Filtro de municípios
    codigos_municipios = sorted(df_municipio["cod_municipio"].dropna().unique())
    municipios_opcoes = {
        f"{MUNICIPIOS_PR.get(cod, 'Desconhecido')} ({cod})": cod 
        for cod in codigos_municipios
    }
    municipios_opcoes_sorted = dict(sorted(municipios_opcoes.items()))
    
    municipios_selecionados_nomes = st.multiselect(
        "Municípios",
        options=list(municipios_opcoes_sorted.keys()),
        default=list(municipios_opcoes_sorted.keys()),
        help="Pesquise e selecione os municípios para análise"
    )
    municipios_selecionados = [
        municipios_opcoes_sorted[nome] for nome in municipios_selecionados_nomes
    ]
    
    # Filtro de faixas etárias
    ORDEM_FAIXA = ["0-4", "5-14", "15-29", "30-44", "45-59", "60-74", "75+", "Não informada"]
    faixas_disponiveis = [f for f in ORDEM_FAIXA if f in df_mortal["faixa_etaria"].unique()]
    
    faixas_selecionadas = st.multiselect(
        "Faixas etárias",
        options=faixas_disponiveis,
        default=faixas_disponiveis,
        help="Selecione as faixas etárias para análise"
    )
    
    st.divider()
    st.caption(
        f"**Dados carregados:**\n"
        f"- {len(df_evolucao):,} registros mensais\n"
        f"- {len(df_municipio['cod_municipio'].unique())} municípios\n"
        f"- {len(tipos)} tipos de leucemia"
    )

# Aplicar filtros
df_evolucao_filtrado = df_evolucao[
    (df_evolucao["ano"] >= ano_min) &
    (df_evolucao["ano"] <= ano_max) &
    (df_evolucao["tipo_leucemia"].isin(tipos_selecionados))
]

df_mortal_filtrado = df_mortal[
    (df_mortal["ano"] >= ano_min) &
    (df_mortal["ano"] <= ano_max) &
    (df_mortal["tipo_leucemia"].isin(tipos_selecionados)) &
    (df_mortal["faixa_etaria"].isin(faixas_selecionadas))
]

df_municipio_filtrado = df_municipio[
    (df_municipio["ano"] >= ano_min) &
    (df_municipio["ano"] <= ano_max) &
    (df_municipio["tipo_leucemia"].isin(tipos_selecionados)) &
    (df_municipio["cod_municipio"].isin(municipios_selecionados))
]

# ── Métricas resumo ───────────────────────────────────────────────────────────
total_internacoes = df_evolucao_filtrado["internacoes"].sum()
total_obitos = df_evolucao_filtrado["obitos"].sum()
taxa_mortalidade = (total_obitos / total_internacoes * 100) if total_internacoes > 0 else 0
total_dias = df_evolucao_filtrado["total_dias_perm"].sum()
media_dias = total_dias / total_internacoes if total_internacoes > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "Total de Internações",
    f"{total_internacoes:,.0f}",
    help="Número total de internações por leucemia no período selecionado"
)
col2.metric(
    "Total de Óbitos",
    f"{total_obitos:,.0f}",
    delta=f"{taxa_mortalidade:.1f}% mortalidade",
    delta_color="inverse",
    help="Número total de óbitos e taxa de mortalidade"
)
col3.metric(
    "Média de Permanência",
    f"{media_dias:.1f} dias",
    help="Dias médios de internação"
)
col4.metric(
    "Período Analisado",
    f"{ano_max - ano_min + 1} anos",
    delta=f"{ano_min}–{ano_max}",
    delta_color="off"
)

st.divider()

# ── P1: Evolução anual de internações ─────────────────────────────────────────
st.subheader("Evolução Temporal de Internações")

col_left, col_right = st.columns([3, 1])

with col_left:
    evolucao_ano = (
        df_evolucao_filtrado
        .groupby("ano", as_index=False)
        .agg(
            internacoes=("internacoes", "sum"),
            obitos=("obitos", "sum"),
            dias_perm=("total_dias_perm", "sum")
        )
        .sort_values("ano")
    )
    evolucao_ano["taxa_mortalidade"] = (
        evolucao_ano["obitos"] / evolucao_ano["internacoes"] * 100
    ).round(2)
    
    fig1 = go.Figure()
    
    # Linha de internações
    fig1.add_trace(go.Scatter(
        x=evolucao_ano["ano"],
        y=evolucao_ano["internacoes"],
        mode="lines+markers",
        name="Internações",
        line=dict(color="#FF6B35", width=3),
        marker=dict(size=8),
        hovertemplate="<b>%{x}</b><br>Internações: %{y:,.0f}<extra></extra>"
    ))
    
    # Linha de óbitos
    fig1.add_trace(go.Scatter(
        x=evolucao_ano["ano"],
        y=evolucao_ano["obitos"],
        mode="lines+markers",
        name="Óbitos",
        line=dict(color="#004E89", width=3, dash="dash"),
        marker=dict(size=8),
        hovertemplate="<b>%{x}</b><br>Óbitos: %{y:,.0f}<extra></extra>"
    ))
    
    fig1.update_layout(
        hovermode="x unified",
        xaxis_title="Ano",
        yaxis_title="Quantidade",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    st.plotly_chart(fig1, use_container_width=True)

with col_right:
    st.metric(
        "Tendência",
        f"{evolucao_ano['internacoes'].iloc[-1]:,.0f}",
        delta=f"{evolucao_ano['internacoes'].iloc[-1] - evolucao_ano['internacoes'].iloc[0]:+,.0f}",
        delta_color="off",
        help=f"Variação entre {ano_min} e {ano_max}"
    )
    
    variacao_pct = (
        (evolucao_ano['internacoes'].iloc[-1] - evolucao_ano['internacoes'].iloc[0]) /
        evolucao_ano['internacoes'].iloc[0] * 100
    )
    
    st.metric(
        "Variação %",
        f"{variacao_pct:+.1f}%",
        help="Variação percentual do primeiro para o último ano"
    )
    
    st.metric(
        "Pico",
        f"{evolucao_ano['internacoes'].max():,.0f}",
        delta=f"em {evolucao_ano.loc[evolucao_ano['internacoes'].idxmax(), 'ano']:.0f}",
        delta_color="off"
    )

st.divider()

# ── P2: Mortalidade por faixa etária ──────────────────────────────────────────
st.subheader("Taxa de Mortalidade por Faixa Etária")

ORDEM_FAIXA = ["0-4","5-14","15-29","30-44","45-59","60-74","75+","Não informada"]

mortal_faixa = (
    df_mortal_filtrado
    .groupby("faixa_etaria", as_index=False)
    .agg(internacoes=("internacoes","sum"), obitos=("obitos","sum"))
)
mortal_faixa["taxa_pct"] = (
    mortal_faixa["obitos"] / mortal_faixa["internacoes"] * 100
).round(1)
mortal_faixa["faixa_etaria"] = pd.Categorical(
    mortal_faixa["faixa_etaria"], categories=ORDEM_FAIXA, ordered=True
)
mortal_faixa = mortal_faixa.sort_values("faixa_etaria")

col_esq, col_dir = st.columns([2, 1])

with col_esq:
    fig2 = px.bar(
        mortal_faixa,
        x="faixa_etaria",
        y="taxa_pct",
        labels={"faixa_etaria": "Faixa Etária", "taxa_pct": "Taxa de Mortalidade (%)"},
        text="taxa_pct",
        color="taxa_pct",
        color_continuous_scale="Reds",
    )
    fig2.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>Mortalidade: %{y:.1f}%<extra></extra>"
    )
    fig2.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Faixa Etária",
        yaxis_title="Taxa de Mortalidade (%)"
    )
    st.plotly_chart(fig2, use_container_width=True)

with col_dir:
    st.markdown("**📊 Análise:**")
    if len(mortal_faixa) > 0:
        faixa_max = mortal_faixa.loc[mortal_faixa["taxa_pct"].idxmax()]
        faixa_min_filtrada = mortal_faixa[mortal_faixa["faixa_etaria"] != "Não informada"]
        if len(faixa_min_filtrada) > 0:
            faixa_min = faixa_min_filtrada["taxa_pct"].idxmin()
            faixa_min_data = mortal_faixa.loc[faixa_min]
        else:
            faixa_min_data = faixa_max
        
        st.info(
            f"**Maior mortalidade:**\n\n"
            f"Faixa {faixa_max['faixa_etaria']}: **{faixa_max['taxa_pct']:.1f}%**\n\n"
            f"{faixa_max['obitos']:.0f} óbitos em {faixa_max['internacoes']:.0f} internações"
        )
        
        st.success(
            f"**Menor mortalidade:**\n\n"
            f"Faixa {faixa_min_data['faixa_etaria']}: **{faixa_min_data['taxa_pct']:.1f}%**\n\n"
            f"{faixa_min_data['obitos']:.0f} óbitos em {faixa_min_data['internacoes']:.0f} internações"
        )
    else:
        st.warning("Sem dados para as faixas etárias selecionadas")

st.divider()

# ── P3: Tipo de leucemia com mais óbitos ──────────────────────────────────────
st.subheader("Tipos de Leucemia com Maior Mortalidade")

obitos_tipo = (
    df_mortal_filtrado
    .groupby("tipo_leucemia", as_index=False)
    .agg(internacoes=("internacoes","sum"), obitos=("obitos","sum"))
)
obitos_tipo["taxa_pct"] = (
    obitos_tipo["obitos"] / obitos_tipo["internacoes"] * 100
).round(1)
obitos_tipo = obitos_tipo.sort_values("obitos", ascending=True).tail(15)

fig3 = px.bar(
    obitos_tipo,
    x="obitos",
    y="tipo_leucemia",
    orientation="h",
    labels={"obitos": "Óbitos", "tipo_leucemia": "Tipo de Leucemia"},
    text="obitos",
    color="taxa_pct",
    color_continuous_scale="Oranges",
)
fig3.update_traces(
    texttemplate="%{text:,.0f}",
    textposition="outside",
    hovertemplate="<b>%{y}</b><br>Óbitos: %{x:,.0f}<br>Taxa: %{marker.color:.1f}%<extra></extra>"
)
fig3.update_layout(
    height=500,
    xaxis_title="Número de Óbitos",
    yaxis_title="",
    coloraxis_colorbar_title="Taxa (%)"
)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── P4: Distribuição por sexo ao longo dos anos ───────────────────────────────
st.subheader("Distribuição por Sexo ao Longo dos Anos")

sexo_ano = (
    df_evolucao_filtrado
    .groupby(["ano", "sexo"], as_index=False)
    .agg(internacoes=("internacoes", "sum"), obitos=("obitos", "sum"))
    .sort_values("ano")
)

# Incluir todos os sexos (Masculino, Feminino, Ignorado)
fig4 = px.line(
    sexo_ano,
    x="ano",
    y="internacoes",
    color="sexo",
    markers=True,
    labels={"ano": "Ano", "internacoes": "Internações", "sexo": "Sexo"},
    color_discrete_map={
        "Masculino": "#1f77b4",
        "Feminino": "#ff7f0e",
        "Ignorado": "#d62728"
    },
)
fig4.update_traces(line=dict(width=3), marker=dict(size=8))
fig4.update_layout(
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=400,
    xaxis_title="Ano",
    yaxis_title="Internações"
)
st.plotly_chart(fig4, use_container_width=True)

# Análise por sexo
col_masc, col_fem, col_ign = st.columns(3)
masc_total = sexo_ano[sexo_ano["sexo"] == "Masculino"]["internacoes"].sum()
fem_total = sexo_ano[sexo_ano["sexo"] == "Feminino"]["internacoes"].sum()
ign_total = sexo_ano[sexo_ano["sexo"] == "Ignorado"]["internacoes"].sum()
total_geral = masc_total + fem_total + ign_total

with col_masc:
    st.metric(
        "Masculino",
        f"{masc_total:,.0f}",
        delta=f"{masc_total/total_geral*100:.1f}%" if total_geral > 0 else "0%",
        delta_color="off"
    )

with col_fem:
    st.metric(
        "Feminino",
        f"{fem_total:,.0f}",
        delta=f"{fem_total/total_geral*100:.1f}%" if total_geral > 0 else "0%",
        delta_color="off"
    )

with col_ign:
    st.metric(
        "Ignorado",
        f"{ign_total:,.0f}",
        delta=f"{ign_total/total_geral*100:.1f}%" if total_geral > 0 else "0%",
        delta_color="off"
    )

st.divider()

# ── P5: Municípios com mais internações ───────────────────────────────────────
st.subheader("Municípios com Mais Internações")

top_n = st.slider(
    "Número de municípios a exibir:",
    min_value=5,
    max_value=30,
    value=15,
    step=5
)

municipios = (
    df_municipio_filtrado
    .groupby("cod_municipio", as_index=False)
    .agg(internacoes=("internacoes","sum"), obitos=("obitos","sum"))
    .sort_values("internacoes", ascending=False)
    .head(top_n)
)

# Mapear nomes de municípios
municipios["nome"] = municipios["cod_municipio"].map(MUNICIPIOS_PR).fillna("Outros")
municipios["label"] = municipios["nome"] + " (" + municipios["cod_municipio"] + ")"
municipios = municipios.sort_values("internacoes", ascending=True)

fig5 = px.bar(
    municipios,
    x="internacoes",
    y="label",
    orientation="h",
    labels={"internacoes": "Internações", "label": "Município"},
    text="internacoes",
    color="internacoes",
    color_continuous_scale="Blues",
)
fig5.update_traces(
    texttemplate="%{text:,.0f}",
    textposition="outside",
    hovertemplate="<b>%{y}</b><br>Internações: %{x:,.0f}<extra></extra>"
)
fig5.update_layout(
    height=max(400, top_n * 30),
    showlegend=False,
    xaxis_title="Número de Internações",
    yaxis_title=""
)
st.plotly_chart(fig5, use_container_width=True)

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    "**LeucemiaPR** · Dados: DATASUS/SIH-SUS · "
    "Stack: Python, DuckDB, Parquet, Streamlit · "
    "CIDs: C91 (Linfoide), C92 (Mieloide), C93 (Monocítica), C94 (Outras), C95 (NE)"
)
