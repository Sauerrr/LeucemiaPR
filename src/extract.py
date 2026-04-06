"""
extract.py
Download dos dados brutos do SIH/SUS via PySUS.
Estado: Paraná (PR) | Período: 2010–2026 | CIDs: C91–C95 (leucemias)
"""

from pathlib import Path
import os
import pandas as pd
from pysus.ftp.databases.sih import SIH

from .monitor import log_etapa

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

# Caminho para o diretório raiz do projeto (pai de src/)
try:
    # Quando executado como módulo importado
    PROJECT_ROOT = Path(__file__).parent.parent
except NameError:
    # Quando executado diretamente (REPL, notebook, exec)
    # Assume que o script está em src/ e sobe um nível
    PROJECT_ROOT = Path(os.getcwd()).parent if Path(os.getcwd()).name == "src" else Path(os.getcwd())

RAW_DIR = PROJECT_ROOT / "data" / "raw"


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


def ler_parquets_do_disco() -> list:
    """
    Lê os arquivos parquet já baixados no diretório data/raw.
    Retorna lista de paths dos arquivos encontrados.
    """
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Diretório {RAW_DIR} não encontrado.")
    
    arquivos = list(RAW_DIR.glob("*.parquet"))
    print(f"Encontrados {len(arquivos)} arquivos parquet em {RAW_DIR}")
    return arquivos


def consolidar_e_filtrar_do_disco(arquivos_parquet: list) -> pd.DataFrame:
    """
    Lê arquivos parquet do disco, seleciona as colunas necessárias
    e filtra apenas registros com CID de leucemia.
    """
    frames = []
    total = len(arquivos_parquet)
    
    for i, arquivo in enumerate(arquivos_parquet, 1):
        try:
            print(f"  [{i:03d}/{total}] Lendo {arquivo.name}...")
            df_raw = pd.read_parquet(arquivo, columns=COLUNAS)
            
            # Filtro robusto: trata NaN, remove espaços, normaliza maiúsculas
            mask = df_raw["DIAG_PRINC"].fillna("").str.strip().str[:3].str.upper().isin(CID_LEUCEMIA)
            filtrado = df_raw[mask]
            if not filtrado.empty:
                frames.append(filtrado)
                print(f"    ✓ Encontrados {len(filtrado)} registros de leucemia")
        except Exception as e:
            print(f"    Aviso: erro ao ler arquivo {arquivo.name} — {e}")
            continue

    if not frames:
        raise ValueError("Nenhum registro de leucemia encontrado nos arquivos baixados.")

    return pd.concat(frames, ignore_index=True)


def consolidar_e_filtrar(parquets: list) -> pd.DataFrame:
    """
    Lê todos os Parquets, seleciona as colunas necessárias
    e filtra apenas registros com CID de leucemia.
    """
    frames = []
    for pq in parquets:
        try:
            df_raw = pq.to_dataframe(columns=COLUNAS)
            
            # Filtro robusto: trata NaN, remove espaços, normaliza maiúsculas
            mask = df_raw["DIAG_PRINC"].fillna("").str.strip().str[:3].str.upper().isin(CID_LEUCEMIA)
            filtrado = df_raw[mask]
            if not filtrado.empty:
                frames.append(filtrado)
                print(f"    ✓ Encontrados {len(filtrado)} registros de leucemia")
        except Exception as e:
            print(f"    Aviso: erro ao ler arquivo — {e}")
            continue

    if not frames:
        raise ValueError("Nenhum registro de leucemia encontrado nos arquivos baixados.")

    return pd.concat(frames, ignore_index=True)


# ── Ponto de entrada ──────────────────────────────────────────────────────────
def run(skip_download: bool = False) -> pd.DataFrame:
    """
    Executa o pipeline de extração completo.
    
    Args:
        skip_download: Se True, pula o download e lê arquivos já salvos em data/raw
    
    Retorna DataFrame bruto com registros de leucemia no PR (2010–2026).
    """
    if skip_download:
        print("Modo skip_download ativo: lendo arquivos já baixados...")
        arquivos = ler_parquets_do_disco()
        
        log_etapa(
            "Extract - Arquivos locais encontrados",
            "OK",
            qtd_depois=len(arquivos),
            obs=f"Lendo de {RAW_DIR}",
        )
        
        print("Consolidando registros e filtrando leucemias...")
        df = consolidar_e_filtrar_do_disco(arquivos)
    else:
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
    from monitor import salvar_log
    # Use skip_download=True para ler arquivos já baixados
    df = run(skip_download=True)
    print(df.head())
    salvar_log()
