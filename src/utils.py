# src/utils.py
# Ferramentas auxiliares de limpeza e padronização.
# Funções:
# - converter_data_hibrida: Trata datas em formato Unix (segundos) ou ISO Strings.
# - limpar_tipos_complexos: Converte dicionários/listas em strings para compatibilidade SQL.
# - enforce_schema: Garante que os DataFrames tenham as colunas exatas definidas no config.py.

import pandas as pd
import logging
from src.config import TABLE_SCHEMAS

logger = logging.getLogger(__name__)

def converter_data_hibrida(coluna, context_name=""):
    num_vals = pd.to_numeric(coluna, errors='coerce')
    mask_unix = (num_vals > 0) & (num_vals < 4102444800)
    d_num = pd.to_datetime(num_vals[mask_unix], unit='s', utc=True)
    d_str = pd.to_datetime(coluna, errors='coerce', dayfirst=True, format='mixed', utc=True)
    
    result = d_num.reindex(coluna.index).fillna(d_str).dt.tz_localize(None)
    
    nat_count = result.isna().sum()
    if nat_count > 0:
        logger.warning(f"TABELA {context_name}: {nat_count} datas invalidas convertidas para NaT.")
    
    return result

def limpar_tipos_complexos(df):
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_copy[col] = df_copy[col].astype(str)
    return df_copy

def enforce_schema(df, table_name):
    """
    Garante que o DataFrame tenha EXATAMENTE as colunas esperadas pelo banco.
    """
    if df.empty: return df
    
    expected_cols = TABLE_SCHEMAS.get(table_name)
    if not expected_cols:
        logger.error(f"Schema nao definido para {table_name}")
        return df

    # Normalizar para lowercase
    df.columns = df.columns.str.lower()
    
    # Adicionar colunas faltantes com valor nulo
    missing_cols = set(expected_cols) - set(df.columns)
    for col in missing_cols:
        df[col] = None
        
    # Filtrar apenas colunas permitidas e reordenar
    df_final = df[expected_cols].copy()
    
    return df_final