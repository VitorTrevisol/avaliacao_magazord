# main.py
import os
import time
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine

# Importacoes dos nossos modulos
from src.database import init_db_schema, create_indexes
from src.extract import extract_data
from src.transform import (
    transform_users, transform_products, transform_sales, transform_dim_date
)
from src.load import upsert_to_postgres

# Configuracao de Logging Centralizada
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def run_etl():
    start_time = time.time()
    mongo_uri = os.getenv('MONGO_URI')
    postgres_uri = os.getenv('POSTGRES_URI')
    mongo_db_name = os.getenv('MONGO_DB', 'raw_data')

    if not mongo_uri or not postgres_uri:
        raise ValueError("Variaveis de ambiente ausentes.")

    mongo_client = None
    engine = None

    try:
        mongo_client = MongoClient(mongo_uri)
        db = mongo_client[mongo_db_name]
        engine = create_engine(postgres_uri)

        # PASSO 1: DDL
        init_db_schema(engine)

        # PASSO 2: EXTRACAO
        u_raw, p_raw, c_raw = extract_data(db)

        # PASSO 3: TRANSFORMACAO
        dim_users = transform_users(u_raw)
        dim_products = transform_products(p_raw)
        fact_sales, fact_sales_items = transform_sales(c_raw)
        dim_date = transform_dim_date(fact_sales)

        # PASSO 4: CONSISTENCIA (Limpeza de IDs orfaos)
        valid_u = set(dim_users['user_id'])
        fact_sales = fact_sales[fact_sales['user_id'].isin(valid_u)]
        fact_sales_items = fact_sales_items[fact_sales_items['user_id'].isin(valid_u)]

        # PASSO 5: CARGA (Ordem Obrigatória)
        upsert_to_postgres(dim_date, 'dim_date', 'date_id', engine)
        upsert_to_postgres(dim_users, 'dim_users', 'user_id', engine)
        upsert_to_postgres(dim_products, 'dim_products', 'product_id', engine)
        upsert_to_postgres(fact_sales, 'fact_sales', 'sale_id', engine)
        upsert_to_postgres(fact_sales_items, 'fact_sales_items', 'item_id', engine)

        # PASSO 6: INDICES FINAIS
        create_indexes(engine)

        logger.info(f"Pipeline concluido com sucesso em {time.time() - start_time:.2f}s.")

    except Exception as e:
        logger.error(f"Erro critico: {e}", exc_info=True)
    finally:
        if mongo_client: mongo_client.close()
        if engine: engine.dispose()

if __name__ == "__main__":
    run_etl()