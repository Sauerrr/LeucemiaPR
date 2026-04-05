"""
extract.py
Download dos dados brutos do SIH/SUS via PySUS.
Estado: Paraná (PR) | Período: 2010–2026 | CIDs: C91–C95 (leucemias)
"""

from pathlib import Path
import pandas as pd
from pysus.ftp.databases.sih import SIH

from src.monitor import log_etapa

# ── Parâmetros ────────────────────────────────────────────────────────────────
ESTADO  = "PR"
ANOS    = list(range(2010, 2027))
MESES   = list(range(1, 13))

CID_LEUCEMIA = ["C91", "C92", "C93", "C94", "C95"]

COLUNAS = [
    "DIAG_PRINC",  # CID-10 do diagnóstico principal
    "MUNIC_RES",   # Município de residência
    "SEXO",        # Sexo do paciente
    "IDADE",       # Idade codificada
    "MORTE",       # Flag de óbito (1 = sim)
    "ANO_CMPT",    # Ano de competência
    "MES_CMPT",    # Mês de competência
    "DIAS_PERM",   # Dias de permanência
]

RAW_DIR = Path("data/raw")


# ── Funções ───────────────────────────────────────────────────────────────────
def conectar() -> SIH:
    """Conecta ao FTP do DATASUS e carrega o índice de arquivos SIH."""
    print("Conectando ao FTP do DATASUS...")
    sih = SIH().load()
    print("Conexão estabelecida.")
    return sih


def listar_arquivos(sih: SIH) -> list:
    """Lista os arquivos disponíveis para o estado, anos e meses configurados."""
    files = sih.get_files(
        group="RD",       # AIH Reduzida — contém diagnóstico, óbito, sexo, idade
        uf=ESTADO,
        year=ANOS,
        month=MESES,
    )
    print(f"Arquivos encontrados: {len(files)}")
    return files


def baixar_em_lotes(sih: SIH, files: list) -> list:
    """
    Baixa os arquivos em lotes por ano para exibir progresso claro.
    Retorna lista de ParquetFile prontos para leitura.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    todos_parquets = []
    anos_disponiveis = sorted(set(sih.format(f)[2] for f in files))
    total_anos = len(anos_disponiveis)

    for i, ano in enumerate(anos_disponiveis, 1):
        arquivos_ano = [f for f in files if sih.format(f)[2] == ano]
        print(f"  [{i:02d}/{total_anos}] Baixando {ano} — {len(arquivos_ano)} arquivo(s)...")

        parquets = sih.download(arquivos_ano, local_dir=str(RAW_DIR))
        if isinstance(parquets, list):
            todos_parquets.extend(parquets)
        else:
            todos_parquets.append(parquets)

    return todos_parquets


def consolidar_e_filtrar(parquets: list) -> pd.DataFrame:
    """
    Lê todos os Parquets, seleciona as colunas necessárias
    e filtra apenas registros com CID de leucemia.
    """
    frames = []
    for pq in parquets:
        try:
            df_raw = pq.to_dataframe(columns=COLUNAS)
            mask = df_raw["DIAG_PRINC"].str[:3].str.upper().isin(CID_LEUCEMIA)
            filtrado = df_raw[mask]
            if not filtrado.empty:
                frames.append(filtrado)
        except Exception as e:
            print(f"    Aviso: erro ao ler arquivo — {e}")
            continue

    if not frames:
        raise ValueError("Nenhum registro de leucemia encontrado nos arquivos baixados.")

    return pd.concat(frames, ignore_index=True)


# ── Ponto de entrada ──────────────────────────────────────────────────────────
def run() -> pd.DataFrame:
    """
    Executa o pipeline de extração completo.
    Retorna DataFrame bruto com registros de leucemia no PR (2010–2026).
    """
    sih   = conectar()
    files = listar_arquivos(sih)

    log_etapa(
        "Extract - Arquivos disponíveis no FTP",
        "OK",
        qtd_depois=len(files),
        obs=f"Estado: {ESTADO} | Anos: {ANOS[0]}-{ANOS[-1]}",
    )

    parquets = baixar_em_lotes(sih, files)

    log_etapa(
        "Extract - Download concluído",
        "OK",
        qtd_depois=len(parquets),
        obs="Arquivos em data/raw/",
    )

    print("Consolidando registros e filtrando leucemias...")
    df = consolidar_e_filtrar(parquets)

    log_etapa(
        "Extract - Leitura e filtro por CID (C91–C95)",
        "OK",
        qtd_depois=len(df),
        obs="Colunas selecionadas para as perguntas de negócio",
    )

    return df


if __name__ == "__main__":
    from src.monitor import salvar_log
    df = run()
    print(df.head())
    salvar_log()