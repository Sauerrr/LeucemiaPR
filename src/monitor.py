"""
monitor.py
Registro e exibição do log de etapas do pipeline LeucemiaPR.
Compatível com o padrão log_etapa() exigido no Projeto_Final.ipynb.
"""

import json
import datetime
from pathlib import Path

import pandas as pd

# ── Estado global do log ─────────────────────────────────────────────────────
PIPELINE_LOG = []


def log_etapa(
    etapa: str,
    status: str,
    qtd_antes: int = None,
    qtd_depois: int = None,
    obs: str = "",
) -> None:
    """
    Registra o resultado de uma etapa do pipeline.

    Parâmetros
    ----------
    etapa     : nome descritivo da etapa  (ex: 'ETL - Remover duplicatas')
    status    : 'OK' ou 'FALHA'
    qtd_antes : número de registros antes da operação
    qtd_depois: número de registros após a operação
    obs       : observação livre
    """
    removidos = (
        (qtd_antes or 0) - (qtd_depois or 0)
        if qtd_antes is not None and qtd_depois is not None
        else None
    )

    entrada = {
        "etapa": etapa,
        "status": status,
        "qtd_antes": qtd_antes,
        "qtd_depois": qtd_depois,
        "removidos": removidos,
        "timestamp": datetime.datetime.now().isoformat(),
        "obs": obs,
    }
    PIPELINE_LOG.append(entrada)

    qtd_str = f"{qtd_depois:,}" if qtd_depois is not None else "-"
    rem_str = f"  ({removidos:+,} registros)" if removidos else ""
    print(f"[{status:^5}]  {etapa:<50}  {qtd_str} registros{rem_str}")


def exibir_log() -> pd.DataFrame:
    """Retorna o log completo do pipeline como DataFrame."""
    if not PIPELINE_LOG:
        print("Nenhuma etapa registrada ainda.")
        return None
    return pd.DataFrame(PIPELINE_LOG)


def resumo_log() -> None:
    """Exibe um resumo consolidado do pipeline."""
    df = exibir_log()
    if df is None:
        return

    ok    = df[df["status"] == "OK"].shape[0]
    falha = df[df["status"] == "FALHA"].shape[0]
    total_removidos = df["removidos"].dropna().sum()

    print("\n" + "=" * 60)
    print("  RESUMO DO PIPELINE")
    print("=" * 60)
    print(f"  Etapas executadas : {len(df)}")
    print(f"  OK                : {ok}")
    print(f"  FALHA             : {falha}")
    print(f"  Registros removidos no pipeline: {int(total_removidos):,}")
    print("=" * 60)


def salvar_log(caminho: str = "logs/pipeline_log.json") -> None:
    """Salva o log completo em JSON para auditoria."""
    Path(caminho).parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(PIPELINE_LOG, f, ensure_ascii=False, indent=2)
    print(f"Log salvo em: {caminho}")


def limpar_log() -> None:
    """Limpa o log em memória (útil para reexecuções parciais)."""
    PIPELINE_LOG.clear()
    print("Log limpo.")