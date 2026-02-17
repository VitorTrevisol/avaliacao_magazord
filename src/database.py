# src/database.py
# Módulo de infraestrutura de banco de dados relacional.
# Funções: 
# - init_db_schema: Executa o DDL para criação das tabelas Fato e Dimensão.
# - create_indexes: Cria índices nas chaves estrangeiras para otimizar JOINS analíticos.
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

def init_db_schema(engine):
    """Cria tabelas já com chaves primárias e estrangeiras definidas."""
    logger.info("Inicializando schema do banco de dados...")
    
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date DATE, day INTEGER, month INTEGER, month_name TEXT,
            year INTEGER, quarter INTEGER, day_of_week TEXT, is_weekend BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_id INTEGER PRIMARY KEY,
            firstname TEXT, lastname TEXT, maidenname TEXT, age INTEGER, gender TEXT,
            email TEXT, phone TEXT, username TEXT, password TEXT, birthdate TEXT,
            image TEXT, bloodgroup TEXT, height NUMERIC(5,2), weight NUMERIC(5,2),
            eyecolor TEXT, hair_color TEXT, hair_type TEXT, ip TEXT, macaddress TEXT,
            university TEXT, useragent TEXT, role TEXT, cpf TEXT, cnpj TEXT,
            address_address TEXT, address_city TEXT, address_state TEXT,
            address_statecode TEXT, address_postalcode TEXT, address_country TEXT,
            address_coordinates_lat NUMERIC(10,6), address_coordinates_lng NUMERIC(10,6),
            bank_cardexpire TEXT, bank_cardnumber TEXT, bank_cardtype TEXT,
            bank_currency TEXT, bank_iban TEXT, company_department TEXT,
            company_name TEXT, company_title TEXT, company_address_address TEXT,
            company_address_city TEXT, company_address_state TEXT,
            company_address_statecode TEXT, company_address_postalcode TEXT,
            company_address_country TEXT, company_address_coordinates_lat NUMERIC(10,6),
            company_address_coordinates_lng NUMERIC(10,6), crypto_coin TEXT,
            crypto_wallet TEXT, crypto_network TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id INTEGER PRIMARY KEY,
            title TEXT, description TEXT, category TEXT, price NUMERIC(10, 2),
            discountpercentage NUMERIC(5, 2), rating NUMERIC(3, 2), stock INTEGER,
            tags TEXT, brand TEXT, sku TEXT, weight INTEGER, warrantyinformation TEXT,
            shippinginformation TEXT, availabilitystatus TEXT, reviews TEXT,
            returnpolicy TEXT, minimumorderquantity INTEGER, images TEXT, thumbnail TEXT,
            dimensions_width NUMERIC(10, 2), dimensions_height NUMERIC(10, 2),
            dimensions_depth NUMERIC(10, 2), meta_createdat TIMESTAMP,
            meta_updatedat TIMESTAMP, meta_barcode TEXT, meta_qrcode TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL, date_id INTEGER NOT NULL,
            total NUMERIC(15, 2), discountedtotal NUMERIC(15, 2),
            totalproducts INTEGER, totalquantity INTEGER,
            transaction_date TIMESTAMP WITHOUT TIME ZONE,
            CONSTRAINT fk_sales_user FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
            CONSTRAINT fk_sales_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_sales_items (
            item_id TEXT PRIMARY KEY,
            sale_id INTEGER NOT NULL, product_id INTEGER NOT NULL,
            user_id INTEGER, quantity INTEGER, price NUMERIC(10, 2),
            total NUMERIC(15, 2), discountpercentage NUMERIC(5, 2),
            discountedtotal NUMERIC(15, 2),
            CONSTRAINT fk_items_sales FOREIGN KEY (sale_id) REFERENCES fact_sales(sale_id),
            CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        );
        """
    ]

    with engine.begin() as conn:
        for sql in ddl_statements:
            conn.execute(text(sql))
    
    logger.info("DDL executado com sucesso.")

def create_indexes(engine):
    queries = [
        "CREATE INDEX IF NOT EXISTS idx_sales_date_id ON fact_sales(date_id);",
        "CREATE INDEX IF NOT EXISTS idx_sales_user ON fact_sales(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_items_product ON fact_sales_items(product_id);"
    ]
    with engine.begin() as conn:
        for q in queries: conn.execute(text(q))