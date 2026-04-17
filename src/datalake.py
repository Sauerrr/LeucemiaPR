"""
datalake.py
Pipeline ELT com arquitetura medallion: Bronze → Silver → Gold.
Toda transformação nas camadas Silver e Gold é feita em SQL via DuckDB.
"""

from pathlib import Path
import os

import duckdb
import pandas as pd

from .monitor import log_etapa

try:
    PROJECT_ROOT = Path(__file__).parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent if Path(os.getcwd()).name == "src" else Path(os.getcwd())

BRONZE_PATH = str(PROJECT_ROOT / "datalake" / "bronze" / "bronze.parquet")
SILVER_PATH = str(PROJECT_ROOT / "datalake" / "silver" / "silver.parquet")
GOLD_DIR    = PROJECT_ROOT / "datalake" / "gold"

GOLD_PATHS  = {
    "gold_evolucao_anual":    str(GOLD_DIR / "gold_evolucao_anual.parquet"),
    "gold_mortalidade_faixa": str(GOLD_DIR / "gold_mortalidade_faixa.parquet"),
    "gold_municipios":        str(GOLD_DIR / "gold_municipios.parquet"),
}


def carregar_bronze(df_raw: pd.DataFrame) -> None:
    """
    Salva o dado bruto como Parquet na camada Bronze.
    Nenhuma transformação — espelho fiel da fonte.
    """
    Path(BRONZE_PATH).parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_parquet(BRONZE_PATH, index=False)
    log_etapa(
        "ELT - Bronze (LOAD)",
        "OK",
        qtd_depois=len(df_raw),
        obs=f"Salvo em {BRONZE_PATH}",
    )


def construir_silver(conn: duckdb.DuckDBPyConnection) -> int:
    """
    Cria a camada Silver com limpeza e transformações.
    Usa SQL para processar e salva via pandas para garantir ordem das colunas.
    """
    Path(SILVER_PATH).parent.mkdir(parents=True, exist_ok=True)

    qtd_bronze = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{BRONZE_PATH}')"
    ).fetchone()[0]

    # Executar transformação SQL
    silver_df = conn.execute(f"""
        SELECT
            UPPER(TRIM(DIAG_PRINC))                          AS cid,
            LPAD(CAST(MUNIC_RES AS VARCHAR), 6, '0')         AS cod_municipio,
            CASE SEXO
                WHEN '1' THEN 'Masculino'
                WHEN '2' THEN 'Feminino'
                ELSE          'Ignorado'
            END                                              AS sexo,
            TRY_CAST(ANO_CMPT  AS INTEGER)                  AS ano,
            TRY_CAST(MES_CMPT  AS INTEGER)                  AS mes,
            TRY_CAST(MORTE     AS INTEGER)                  AS obito,
            GREATEST(TRY_CAST(TRIM(DIAS_PERM) AS INTEGER), 0) AS dias_perm,
            COALESCE(TRY_CAST(TRIM(IDADE) AS INTEGER), 0)   AS idade_anos,
            CASE
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 0  AND 4   THEN '0-4'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 5  AND 14  THEN '5-14'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 15 AND 29  THEN '15-29'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 30 AND 44  THEN '30-44'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 45 AND 59  THEN '45-59'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) BETWEEN 60 AND 74  THEN '60-74'
                WHEN TRY_CAST(TRIM(IDADE) AS INTEGER) >= 75               THEN '75+'
                ELSE 'Não informada'
            END                                              AS faixa_etaria,
            CASE SUBSTRING(UPPER(TRIM(DIAG_PRINC)), 1, 3)
                WHEN 'C91' THEN 'Linfoide'
                WHEN 'C92' THEN 'Mieloide'
                WHEN 'C93' THEN 'Monocítica'
                WHEN 'C94' THEN 'Outras especificadas'
                WHEN 'C95' THEN 'Não especificada'
                ELSE            'Não identificado'
            END                                              AS grupo_leucemia,
            CASE UPPER(TRIM(DIAG_PRINC))
                WHEN 'C910' THEN 'LLA'         WHEN 'C911' THEN 'LLC'
                WHEN 'C920' THEN 'LMA'         WHEN 'C921' THEN 'LMC'
                WHEN 'C959' THEN 'Leucemia NE'
                ELSE UPPER(TRIM(DIAG_PRINC))
            END                                              AS tipo_leucemia
        FROM read_parquet('{BRONZE_PATH}')
        WHERE TRY_CAST(ANO_CMPT  AS INTEGER) IS NOT NULL
          AND TRY_CAST(MES_CMPT  AS INTEGER) IS NOT NULL
          AND TRIM(DIAG_PRINC)              IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY DIAG_PRINC, MUNIC_RES, SEXO, IDADE, ANO_CMPT, MES_CMPT, MORTE, DIAS_PERM
            ORDER BY DIAG_PRINC
        ) = 1
    """).fetchdf()

    silver_df.to_parquet(SILVER_PATH, index=False)
    
    qtd_silver = len(silver_df)

    log_etapa(
        "ELT - Silver (TRANSFORM SQL)",
        "OK",
        qtd_antes=qtd_bronze,
        qtd_depois=qtd_silver,
        obs="Limpeza, tipagem e deduplicação via SQL",
    )
    return qtd_silver


SQLS_GOLD = {
    "gold_evolucao_anual": f"""
        SELECT
            ano,
            mes,
            sexo,
            tipo_leucemia,
            grupo_leucemia,
            COUNT(*)        AS internacoes,
            SUM(obito)      AS obitos,
            SUM(dias_perm)  AS total_dias_perm
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY ano, mes, sexo, tipo_leucemia, grupo_leucemia
        ORDER BY ano, mes
    """,

    "gold_mortalidade_faixa": f"""
        SELECT
            ano,
            faixa_etaria,
            tipo_leucemia,
            grupo_leucemia,
            sexo,
            COUNT(*)                                   AS internacoes,
            SUM(obito)                                 AS obitos,
            ROUND(SUM(obito) * 100.0 / COUNT(*), 2)   AS taxa_mortalidade_pct
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY ano,faixa_etaria, tipo_leucemia, grupo_leucemia, sexo
        ORDER BY obitos DESC
    """,

    "gold_municipios": f"""
        SELECT
            cod_municipio,
            tipo_leucemia,
            grupo_leucemia,
            ano,
            COUNT(*)        AS internacoes,
            SUM(obito)      AS obitos
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY cod_municipio, tipo_leucemia, grupo_leucemia, ano
        ORDER BY internacoes DESC
    """,
}


def construir_gold(conn: duckdb.DuckDBPyConnection) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    for nome, sql in SQLS_GOLD.items():
        
        gold_df = conn.execute(sql).fetchdf()
        gold_df.to_parquet(GOLD_PATHS[nome], index=False)
        
        log_etapa(f"ELT - Gold ({nome})", "OK", qtd_depois=len(gold_df))


def validar_cruzado(
    conn: duckdb.DuckDBPyConnection,
    conn_dw: duckdb.DuckDBPyConnection,
) -> None:
    """
    Compara totais entre a fato do DW e a camada Gold.
    Internações e óbitos devem ser iguais após deduplicação.
    """
    total_dw_int = conn_dw.execute(
        "SELECT SUM(internacoes) FROM fato_internacao"
    ).fetchone()[0]

    total_dw_obi = conn_dw.execute(
        "SELECT SUM(obitos) FROM fato_internacao"
    ).fetchone()[0]

    total_gold_int = conn.execute(
        f"SELECT SUM(internacoes) FROM read_parquet('{GOLD_PATHS['gold_evolucao_anual']}')"
    ).fetchone()[0]

    total_gold_obi = conn.execute(
        f"SELECT SUM(obitos) FROM read_parquet('{GOLD_PATHS['gold_evolucao_anual']}')"
    ).fetchone()[0]

    ok_int = total_dw_int == total_gold_int
    ok_obi = total_dw_obi == total_gold_obi

    log_etapa(
        "Validação cruzada - Internações DW × Gold",
        "OK" if ok_int else "FALHA",
        obs=f"DW: {total_dw_int} | Gold: {total_gold_int}",
    )
    log_etapa(
        "Validação cruzada - Óbitos DW × Gold",
        "OK" if ok_obi else "FALHA",
        obs=f"DW: {total_dw_obi} | Gold: {total_gold_obi}",
    )


def run(df_raw: pd.DataFrame, conn_dw: duckdb.DuckDBPyConnection) -> None:
    """
    Executa o pipeline ELT completo.
    Recebe o DataFrame bruto e a conexão do DW para validação cruzada.
    """
    conn = duckdb.connect()

    carregar_bronze(df_raw)
    construir_silver(conn)
    construir_gold(conn)
    validar_cruzado(conn, conn_dw)

    conn.close()
