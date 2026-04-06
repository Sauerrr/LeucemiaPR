"""
main.py
Orquestrador do pipeline completo LeucemiaPR.
Executa: Extract → Transform → Load (DW) → ELT (Data Lake) → Log
"""

import sys
from src.monitor import log_etapa, resumo_log, salvar_log, limpar_log
from src import extract, transform, load, datalake


def main():
    limpar_log()

    print("=" * 60)
    print("  LeucemiaPR — Pipeline de Dados")
    print("=" * 60)

    # ── 1. Extract ────────────────────────────────────────────────
    print("\n[1/4] EXTRACT — Leitura dos dados já baixados")
    try:
        df_raw = extract.run(skip_download=True)
    except Exception as e:
        log_etapa("Extract", "FALHA", obs=str(e))
        print(f"Erro na extração: {e}")
        sys.exit(1)

    # ── 2. Transform ──────────────────────────────────────────────
    print("\n[2/4] TRANSFORM — Limpeza e modelo dimensional")
    try:
        tabelas = transform.run(df_raw)
    except Exception as e:
        log_etapa("Transform", "FALHA", obs=str(e))
        print(f"Erro na transformação: {e}")
        sys.exit(1)

    # ── 3. Load ───────────────────────────────────────────────────
    print("\n[3/4] LOAD — Carga no Data Warehouse (DuckDB)")
    try:
        conn_dw = load.run(tabelas)
    except Exception as e:
        log_etapa("Load", "FALHA", obs=str(e))
        print(f"Erro na carga do DW: {e}")
        sys.exit(1)

    # ── 4. Data Lake ──────────────────────────────────────────────
    print("\n[4/4] ELT — Pipeline Medallion (Bronze → Silver → Gold)")
    try:
        datalake.run(df_raw, conn_dw)
    except Exception as e:
        log_etapa("Data Lake", "FALHA", obs=str(e))
        print(f"Erro no Data Lake: {e}")
        sys.exit(1)
    finally:
        conn_dw.close()

    # ── Resumo e log ──────────────────────────────────────────────
    print()
    resumo_log()
    salvar_log()
    print("\nPipeline concluído. Execute 'streamlit run app.py' para o dashboard.")


if __name__ == "__main__":
    main()
