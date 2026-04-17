# LeucemiaPR

Análise de internações por leucemia no SUS, com foco em padrões regionais, mortalidade e distribuição demográfica entre 2010 e 2026. O projeto implementa um ecossistema completo de dados, incluindo Data Warehouse com modelagem dimensional e Data Lake com arquitetura medallion, finalizando em um dashboard interativo.

---

## 🔗 Links

* **Landing Page:** [https://luan-bs.github.io/LeucemiaPR/](https://luan-bs.github.io/LeucemiaPR/)
* **Dashboard (Streamlit):** [https://leucemia-pr-analysis.streamlit.app/](https://leucemia-pr-analysis.streamlit.app/)

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

## Problemas de Qualidade Tratados

Durante o processo ETL, foram identificados e corrigidos diversos problemas de qualidade nos dados do DATASUS:

### **1. Duplicatas (2.527 registros — 5,7% do dataset)**
* Registros idênticos em todas as colunas
* **Solução:** Remoção via `drop_duplicates()` e `QUALIFY ROW_NUMBER()` no SQL

### **2. Códigos de Sexo Inconsistentes**
* Valores esperados: 1 (Masculino), 2 (Feminino)
* **Problema:** Registros com código "0" ou valores inválidos
* **Solução:** Mapeamento robusto com categoria "Ignorado" para códigos fora do padrão

### **3. Valores Nulos em Campos Críticos**
* Registros sem ano, mês ou CID principal (impossíveis de analisar)
* **Solução:** Remoção apenas de registros sem `ANO_CMPT`, `MES_CMPT` ou `DIAG_PRINC`

### **4. Dias de Permanência Negativos**
* Valores negativos ou nulos na coluna `DIAS_PERM`
* **Solução:** `clip(lower=0)` para garantir valores >= 0, preenchendo nulos com 0

### **5. CIDs com Espaços e Maiúsculas/Minúsculas**
* Diagnósticos registrados como "c920", " C920", "C920 "
* **Solução:** Normalização via `UPPER(TRIM())` para garantir consistência

### **6. Códigos de Município Incompletos**
* Municípios com códigos de 4 ou 5 dígitos ao invés de 6
* **Solução:** Preenchimento com zeros à esquerda via `zfill(6)` / `LPAD()`

### **7. Registros Fora do Escopo Geográfico**
* Internações de residentes de outros estados no PR
* **Solução:** Mapeamento UF via 2 primeiros dígitos do município, categoria "Outro" para estados não mapeados

**Resultado:** De 44.195 registros brutos, 41.668 foram validados e carregados (94,3% de aproveitamento).

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
| Processamento SQL | DuckDB |
| Desenvolvimento | Databricks Community Edition |
| Dashboard | Streamlit |
| Deploy | Streamlit Cloud |
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
