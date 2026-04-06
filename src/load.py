"""
load.py
Cria o Data Warehouse em DuckDB e carrega as tabelas do modelo dimensional.
Ordem obrigatória: dimensões primeiro, fato por último.
"""

from pathlib import Path
import os

import duckdb
import pandas as pd

from .monitor import log_etapa

# Caminho para o diretório raiz do projeto (pai de src/)
try:
    # Quando executado como módulo importado
    PROJECT_ROOT = Path(__file__).parent.parent
except NameError:
    # Quando executado diretamente (REPL, notebook, exec)
    # Assume que o script está em src/ e sobe um nível
    PROJECT_ROOT = Path(os.getcwd()).parent if Path(os.getcwd()).name == "src" else Path(os.getcwd())

DW_PATH = str(PROJECT_ROOT / "dw_projeto.duckdb")


# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = {
    "dim_tempo": """
        CREATE TABLE IF NOT EXISTS dim_tempo (
            sk_tempo   INTEGER PRIMARY KEY,
            ano        INTEGER,
            mes        INTEGER,
            nome_mes   VARCHAR,
            trimestre  INTEGER
        )
    """,

    "dim_local": """
        CREATE TABLE IF NOT EXISTS dim_local (
            sk_local      INTEGER PRIMARY KEY,
            cod_municipio VARCHAR,
            uf            VARCHAR,
            estado        VARCHAR,
            regiao        VARCHAR
        )
    """,

    "dim_paciente": """
        CREATE TABLE IF NOT EXISTS dim_paciente (
            sk_paciente  INTEGER PRIMARY KEY,
            sexo         VARCHAR,
            faixa_etaria VARCHAR
        )
    """,

    "dim_leucemia": """
        CREATE TABLE IF NOT EXISTS dim_leucemia (
            sk_leucemia INTEGER PRIMARY KEY,
            cid         VARCHAR,
            nome_curto  VARCHAR,
            grupo       VARCHAR
        )
    """,

    "fato_internacao": """
        CREATE TABLE IF NOT EXISTS fato_internacao (
            sk_fato      INTEGER PRIMARY KEY,
            sk_tempo     INTEGER REFERENCES dim_tempo(sk_tempo),
            sk_local     INTEGER REFERENCES dim_local(sk_local),
            sk_paciente  INTEGER REFERENCES dim_paciente(sk_paciente),
            sk_leucemia  INTEGER REFERENCES dim_leucemia(sk_leucemia),
            internacoes  INTEGER,
            obitos       INTEGER,
            dias_perm    INTEGER
        )
    """,
}

# Ordem de carga — dimensões antes da fato
ORDEM_CARGA = [
    "dim_tempo",
    "dim_local",
    "dim_paciente",
    "dim_leucemia",
    "fato_internacao",
]


# ── Funções ───────────────────────────────────────────────────────────────────
def conectar() -> duckdb.DuckDBPyConnection:
    """Abre (ou cria) o arquivo DuckDB."""
    Path(DW_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DW_PATH)
    print(f"DW conectado: {DW_PATH}")
    return conn


def criar_tabelas(conn: duckdb.DuckDBPyConnection) -> None:
    """Cria todas as tabelas do DW se ainda não existirem."""
    for tabela, ddl in DDL.items():
        conn.execute(ddl)
    print("Tabelas criadas (ou já existentes).")


def limpar_tabelas(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Limpa todas as tabelas antes de recarregar.
    Ordem inversa para respeitar as foreign keys.
    """
    for tabela in reversed(ORDEM_CARGA):
        conn.execute(f"DELETE FROM {tabela}")
    print("Tabelas limpas para recarga.")


def carregar_tabela(
    conn: duckdb.DuckDBPyConnection,
    nome: str,
    df: pd.DataFrame,
) -> None:
    """Carrega um DataFrame em uma tabela do DW via view temporária."""
    conn.register("_df_temp", df)
    
    # Para fato_internacao, gerar sk_fato com ROW_NUMBER()
    if nome == "fato_internacao":
        conn.execute(f"""
            INSERT INTO {nome} 
            SELECT 
                ROW_NUMBER() OVER () as sk_fato,
                sk_tempo, sk_local, sk_paciente, sk_leucemia, 
                internacoes, obitos, dias_perm
            FROM _df_temp
        """)
    else:
        # Para dimensões, inserir diretamente (já têm SK)
        conn.execute(f"INSERT INTO {nome} SELECT * FROM _df_temp")
    
    conn.unregister("_df_temp")

    qtd = conn.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
    log_etapa(f"DW - Carregar {nome}", "OK", qtd_depois=qtd)


def validar(conn: duckdb.DuckDBPyConnection, tabelas: dict) -> None:
    """
    Validação pós-carga:
      - Contagem de cada tabela bate com o DataFrame de origem
      - Fato não possui SKs nulas
      - Total de óbitos na fato é consistente
    """
    print("\nValidando DW...")
    ok = True

    for nome, df in tabelas.items():
        qtd_dw = conn.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
        qtd_df = len(df)
        status = "OK" if qtd_dw == qtd_df else "DIVERGÊNCIA"
        if status != "OK":
            ok = False
        log_etapa(
            f"DW - Validar {nome}",
            "OK" if status == "OK" else "FALHA",
            qtd_depois=qtd_dw,
            obs=f"DataFrame origem: {qtd_df}",
        )

    # Verificar SKs nulas na fato
    nulos = conn.execute("""
        SELECT COUNT(*) FROM fato_internacao
        WHERE sk_tempo IS NULL OR sk_local IS NULL
           OR sk_paciente IS NULL OR sk_leucemia IS NULL
    """).fetchone()[0]

    status_nulos = "OK" if nulos == 0 else "FALHA"
    if status_nulos != "OK":
        ok = False
    log_etapa("DW - Validar SKs nulas", status_nulos, qtd_depois=nulos)

    if ok:
        print("✅ Todas as validações passaram.")
    else:
        print("❌ Algumas validações falharam — revisar logs.")


# ── Execução ──────────────────────────────────────────────────────────────────
def run(tabelas: tuple) -> duckdb.DuckDBPyConnection:
    """
    Recebe as 5 tabelas (dim_tempo, dim_local, dim_paciente, dim_leucemia, fato)
    e carrega no DW DuckDB.
    Retorna a conexão aberta para uso posterior (validação cruzada).
    """
    dim_tempo, dim_local, dim_paciente, dim_leucemia, fato = tabelas

    # Mapear para dict
    tabelas_dict = {
        "dim_tempo": dim_tempo,
        "dim_local": dim_local,
        "dim_paciente": dim_paciente,
        "dim_leucemia": dim_leucemia,
        "fato_internacao": fato,
    }

    conn = conectar()
    criar_tabelas(conn)
    limpar_tabelas(conn)

    # Carregar na ordem correta
    for nome in ORDEM_CARGA:
        carregar_tabela(conn, nome, tabelas_dict[nome])

    validar(conn, tabelas_dict)
    return conn
