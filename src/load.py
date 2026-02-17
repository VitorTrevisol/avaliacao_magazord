# src/load.py
# Módulo responsável por persistir os dados no PostgreSQL.
# Função: upsert_to_postgres -> Implementa a lógica de idempotência. Utiliza tabelas temporárias 
# e 'ON CONFLICT' para garantir que execuções repetidas não dupliquem dados.

import time
import logging
from sqlalchemy import text
from src.utils import limpar_tipos_complexos, enforce_schema

logger = logging.getLogger(__name__)

def upsert_to_postgres(df, table_name, pk_col, engine):
    if df.empty: return

    df_sql = limpar_tipos_complexos(df)

    df_sql = enforce_schema(df_sql, table_name)

    content_cols = [
        col for col in df_sql.columns 
        if col != pk_col and not col.endswith('_id')
    ]
    if not content_cols:
        content_cols = [col for col in df_sql.columns if col != pk_col]

    if content_cols:
        df_sql = df_sql.drop_duplicates(subset=content_cols, keep='first')

    temp_table = f"temp_{table_name}_{int(time.time())}"
    columns_clause = ", ".join([f'"{col}"' for col in df_sql.columns])
    
    dedup_conditions = [f'target."{col}" IS NOT DISTINCT FROM source."{col}"' for col in content_cols]
    dedup_clause = " AND ".join(dedup_conditions)
    
    upsert_query = f"""
        INSERT INTO "{table_name}" ({columns_clause})
        SELECT {columns_clause} 
        FROM "{temp_table}" source
        WHERE NOT EXISTS (
            SELECT 1 FROM "{table_name}" target
            WHERE {dedup_clause}
        )
        ON CONFLICT ("{pk_col}") DO NOTHING;
    """

    with engine.begin() as conn:
        df_sql.to_sql(temp_table, conn, if_exists='replace', index=False)
        conn.execute(text(upsert_query))
        conn.execute(text(f'DROP TABLE "{temp_table}";'))
        
    logger.info(f"Carga em '{table_name}': {len(df_sql)} registros processados.")