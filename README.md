# LeucemiaPR

Análise de internações por leucemia no SUS, com foco em padrões regionais, mortalidade e distribuição demográfica entre 2010 e 2026. O projeto implementa um ecossistema completo de dados, incluindo Data Warehouse com modelagem dimensional e Data Lake com arquitetura medallion, finalizando em um dashboard interativo.

---

## Fonte dos dados

**Sistema de Informações Hospitalares do SUS (SIH/SUS)**
Disponibilizado pelo DATASUS — Ministério da Saúde
CIDs considerados: C91 a C95 (leucemias — CID-10)
Estado: Paraná (PR)
Período: 2010 a 2026

---

## Perguntas de negócio

1. Qual a evolução anual de internações por leucemia no PR entre 2010 e 2026?
2. Como a taxa de mortalidade por leucemia varia entre faixas etárias?
3. Qual o tipo de leucemia com maior número de óbitos?
4. Existe diferença na distribuição de casos entre sexos ao longo dos anos?
5. Quais municípios do PR concentram mais internações por leucemia?

---

## Arquitetura

```
extract.py          Extração dos dados brutos via PySUS (DATASUS/FTP)
    ↓
data/raw/           Dados brutos por mês/ano (.parquet)
    ↓
src/transform.py    Limpeza, tipagem e construção do modelo dimensional
    ↓
dw_projeto.duckdb   Data Warehouse (star schema — DuckDB)
    ↓
src/datalake.py     Pipeline ELT: Bronze → Silver (SQL) → Gold (SQL)
    ↓
datalake/gold/      Tabelas analíticas finais (.parquet) — servem o dashboard
    ↓
app.py              Dashboard Streamlit
```

---

## Estrutura do repositório

```
LeucemiaPR/
├── data/
│   └── raw/                        # arquivos brutos (.gitignore)
├── datalake/
│   ├── bronze/                     # espelho da fonte (.gitignore)
│   ├── silver/                     # dado limpo via SQL (.gitignore)
│   └── gold/
│       ├── gold_evolucao_anual.parquet
│       ├── gold_mortalidade_faixa.parquet
│       └── gold_municipios.parquet
├── logs/                           # log do pipeline (.gitignore)
├── docs/
│   └── Projeto_Final.ipynb         # notebook de referência acadêmica
├── src/
│   ├── extract.py                  # download SIH via PySUS
│   ├── transform.py                # ETL para o DW
│   ├── load.py                     # carga no DuckDB
│   ├── datalake.py                 # pipeline ELT medallion
│   └── monitor.py                  # log de etapas do pipeline
├── app.py                          # dashboard Streamlit
├── main.py                         # orquestrador do pipeline completo
├── dw_projeto.duckdb               # DW local (.gitignore)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Como executar

**1. Instale as dependências**
```bash
pip install -r requirements.txt
```

**2. Execute o pipeline completo**
```bash
python main.py
```

Isso roda a extração, transformação, carga no DW e geração das tabelas Gold em sequência. O progresso de cada etapa é registrado em `logs/pipeline_log.json`.

**3. Inicie o dashboard**
```bash
streamlit run app.py
```

> A extração (step 1) requer conexão com o FTP do DATASUS e pode levar alguns minutos. Os arquivos Gold já estão versionados no repositório — é possível pular direto para o dashboard sem rodar o pipeline.

---

## Stack

| Camada | Tecnologia |
|---|---|
| Extração | PySUS |
| Armazenamento | Parquet (PyArrow) |
| Data Warehouse | DuckDB |
| Data Lake | DuckDB + Parquet (medallion) |
| Dashboard | Streamlit |
| Linguagem | Python 3.11+ |

---

## .gitignore recomendado

```
data/raw/
datalake/bronze/
datalake/silver/
logs/
dw_projeto.duckdb
__pycache__/
.env
*.pyc
```
