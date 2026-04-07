"""
transform.py
Limpeza, tipagem e construção do modelo dimensional (star schema).
Entrada : DataFrame bruto vindo do extract.py
Saída   : dim_tempo, dim_local, dim_paciente, dim_leucemia, fato_internacao
"""

import pandas as pd
from typing import Optional

from .monitor import log_etapa

# ── Mapeamentos ───────────────────────────────────────────────────────────────
NOMES_LEUCEMIA = {
    "C910": "LLA",              "C911": "LLC",
    "C912": "LLA outras",       "C913": "LLA B-other",
    "C914": "LLA T",
    "C920": "LMA",              "C921": "LMC",
    "C922": "LMA subcategoria", "C923": "LMA M3",
    "C924": "LMA M4",           "C925": "LMA M5",
    "C926": "Eritroleucemia",   "C927": "LMA M7",
    "C929": "LMA NE",
    "C930": "Monocítica aguda", "C931": "Monocítica crônica",
    "C939": "Monocítica NE",
    "C940": "Eritremia aguda",  "C941": "Policitemia vera",
    "C942": "Megacarioblástica aguda", "C943": "Mastocitária",
    "C950": "Leucemia aguda NE","C951": "Leucemia crônica NE",
    "C959": "Leucemia NE",
}

GRUPO_CID = {
    "C91": "Linfoide",
    "C92": "Mieloide",
    "C93": "Monocítica",
    "C94": "Outras especificadas",
    "C95": "Não especificada",
}

MESES_NOME = {
    1:"Janeiro", 2:"Fevereiro", 3:"Março",    4:"Abril",
    5:"Maio",    6:"Junho",     7:"Julho",     8:"Agosto",
    9:"Setembro",10:"Outubro",  11:"Novembro", 12:"Dezembro",
}

CODUF_MUNICIPIO = {
    "41": "PR", "35": "SP", "33": "RJ", "31": "MG",
    "43": "RS", "42": "SC", "29": "BA", "26": "PE",
    "23": "CE", "52": "GO",
}

BINS_IDADE   = [0,  4, 14, 29, 44, 59, 74, 200]
LABELS_IDADE = ["0-4","5-14","15-29","30-44","45-59","60-74","75+"]


# ── 1. Qualidade ──────────────────────────────────────────────────────────────
def remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.drop_duplicates()
    log_etapa("ETL - Remover duplicatas", "OK", qtd_antes=antes, qtd_depois=len(df))
    return df


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    df["ANO_CMPT"]  = pd.to_numeric(df["ANO_CMPT"],  errors="coerce")
    df["MES_CMPT"]  = pd.to_numeric(df["MES_CMPT"],  errors="coerce")
    df["DIAS_PERM"] = pd.to_numeric(df["DIAS_PERM"],  errors="coerce")
    df["MORTE"]     = pd.to_numeric(df["MORTE"],      errors="coerce").fillna(0).astype(int)
    df["DIAG_PRINC"]= df["DIAG_PRINC"].str.upper().str.strip()
    df["SEXO"]      = df["SEXO"].astype(str).str.strip()
    df["MUNIC_RES"] = df["MUNIC_RES"].astype(str).str.zfill(6)
    log_etapa("ETL - Converter tipos", "OK", qtd_depois=len(df))
    return df


def tratar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)

    # Remove linhas sem ano, mês ou CID — sem esses campos não há análise possível
    df = df.dropna(subset=["ANO_CMPT", "MES_CMPT", "DIAG_PRINC"])

    # Sexo ignorado mantido como categoria "Ignorado"
    df["SEXO"] = df["SEXO"].replace({"0": "Ignorado", "1": "Masculino", "3": "Feminino"})
    df["SEXO"] = df["SEXO"].where(df["SEXO"].isin(["Masculino","Feminino","Ignorado"]), "Ignorado")

    # Dias de permanência negativos ou nulos → 0
    df["DIAS_PERM"] = df["DIAS_PERM"].clip(lower=0).fillna(0).astype(int)

    log_etapa("ETL - Tratar nulos e inválidos", "OK", qtd_antes=antes, qtd_depois=len(df))
    return df


def adicionar_idade(df: pd.DataFrame) -> pd.DataFrame:
    """
    O PySUS já retorna o campo IDADE decodificado em anos (0-99).
    Não é necessário decodificar 4XX/3XX.
    """
    df["IDADE_ANOS"] = pd.to_numeric(df["IDADE"], errors="coerce").fillna(0).astype(int)
    df["FAIXA_ETARIA"] = pd.cut(
        df["IDADE_ANOS"], bins=BINS_IDADE, labels=LABELS_IDADE, right=True
    ).astype(str)
    df["FAIXA_ETARIA"] = df["FAIXA_ETARIA"].replace("nan", "Não informada")
    log_etapa("ETL - Decodificar idade e faixa etária", "OK", qtd_depois=len(df))
    return df


def adicionar_municipio(df: pd.DataFrame) -> pd.DataFrame:
    df["UF_RES"] = df["MUNIC_RES"].str[:2].map(CODUF_MUNICIPIO).fillna("Outro")
    df["COD_MUNICIPIO"] = df["MUNIC_RES"]
    log_etapa("ETL - Derivar UF do município", "OK", qtd_depois=len(df))
    return df


# ── 2. Dimensões ──────────────────────────────────────────────────────────────
def build_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["ANO_CMPT", "MES_CMPT"]]
        .drop_duplicates()
        .dropna()
        .sort_values(["ANO_CMPT", "MES_CMPT"])
        .reset_index(drop=True)
    )
    dim.columns = ["ano", "mes"]
    dim["ano"]       = dim["ano"].astype(int)
    dim["mes"]       = dim["mes"].astype(int)
    dim["nome_mes"]  = dim["mes"].map(MESES_NOME)
    dim["trimestre"] = ((dim["mes"] - 1) // 3 + 1)
    dim["sk_tempo"]  = dim.index + 1
    log_etapa("ETL - Construir dim_tempo", "OK", qtd_depois=len(dim))
    return dim[["sk_tempo","ano","mes","nome_mes","trimestre"]]


def build_dim_local(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["COD_MUNICIPIO", "UF_RES"]]
        .drop_duplicates()
        .dropna()
        .sort_values("COD_MUNICIPIO")
        .reset_index(drop=True)
    )
    dim.columns      = ["cod_municipio","uf"]
    dim["estado"]    = "Paraná"
    dim["regiao"]    = "Sul"
    dim["sk_local"]  = dim.index + 1
    log_etapa("ETL - Construir dim_local", "OK", qtd_depois=len(dim))
    return dim[["sk_local","cod_municipio","uf","estado","regiao"]]


def build_dim_paciente(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["SEXO", "FAIXA_ETARIA"]]
        .drop_duplicates()
        .sort_values(["SEXO","FAIXA_ETARIA"])
        .reset_index(drop=True)
    )
    dim.columns        = ["sexo","faixa_etaria"]
    dim["sk_paciente"] = dim.index + 1
    log_etapa("ETL - Construir dim_paciente", "OK", qtd_depois=len(dim))
    return dim[["sk_paciente","sexo","faixa_etaria"]]


def build_dim_leucemia(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["DIAG_PRINC"]]
        .drop_duplicates()
        .sort_values("DIAG_PRINC")
        .reset_index(drop=True)
    )
    dim.columns          = ["cid"]
    dim["nome_curto"]    = dim["cid"].map(NOMES_LEUCEMIA).fillna("Outro")
    dim["grupo"]         = dim["cid"].str[:3].map(GRUPO_CID).fillna("Não identificado")
    dim["sk_leucemia"]   = dim.index + 1
    log_etapa("ETL - Construir dim_leucemia", "OK", qtd_depois=len(dim))
    return dim[["sk_leucemia","cid","nome_curto","grupo"]]


# ── 3. Fato ───────────────────────────────────────────────────────────────────
def build_fato(
    df: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dim_local: pd.DataFrame,
    dim_paciente: pd.DataFrame,
    dim_leucemia: pd.DataFrame,
) -> pd.DataFrame:
    fato = df.copy()

    # Juntar surrogate keys
    fato = fato.merge(
        dim_tempo[["ano","mes","sk_tempo"]],
        left_on=["ANO_CMPT","MES_CMPT"],
        right_on=["ano","mes"],
        how="left"
    )
    fato = fato.merge(
        dim_local[["cod_municipio","sk_local"]],
        left_on="COD_MUNICIPIO",
        right_on="cod_municipio",
        how="left"
    )
    fato = fato.merge(
        dim_paciente[["sexo","faixa_etaria","sk_paciente"]],
        left_on=["SEXO","FAIXA_ETARIA"],
        right_on=["sexo","faixa_etaria"],
        how="left"
    )
    fato = fato.merge(
        dim_leucemia[["cid","sk_leucemia"]],
        left_on="DIAG_PRINC",
        right_on="cid",
        how="left"
    )

    # Métricas
    fato["internacoes"] = 1
    fato["obitos"]      = fato["MORTE"]
    fato["dias_perm"]   = fato["DIAS_PERM"]

    # Selecionar colunas finais e adicionar sk_fato
    fato_final = fato[[
        "sk_tempo","sk_local","sk_paciente","sk_leucemia",
        "internacoes","obitos","dias_perm"
    ]].copy()
    
    # Adicionar sk_fato como chave primária auto-incremental
    fato_final.insert(0, "sk_fato", range(1, len(fato_final) + 1))

    log_etapa("ETL - Construir fato_internacao", "OK", qtd_depois=len(fato_final))
    return fato_final


# ── Pipeline completo ─────────────────────────────────────────────────────────
def run(df: pd.DataFrame) -> tuple:
    """
    Executa o pipeline ETL completo.
    Retorna (dim_tempo, dim_local, dim_paciente, dim_leucemia, fato_internacao)
    """
    df = remover_duplicatas(df)
    df = converter_tipos(df)
    df = tratar_nulos(df)
    df = adicionar_idade(df)
    df = adicionar_municipio(df)

    dim_tempo    = build_dim_tempo(df)
    dim_local    = build_dim_local(df)
    dim_paciente = build_dim_paciente(df)
    dim_leucemia = build_dim_leucemia(df)
    fato         = build_fato(df, dim_tempo, dim_local, dim_paciente, dim_leucemia)

    return dim_tempo, dim_local, dim_paciente, dim_leucemia, fato
