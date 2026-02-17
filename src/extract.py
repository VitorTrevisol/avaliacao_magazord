# src/extract.py
# Módulo responsável por extrair dados brutos das coleções do MongoDB.
# Função: extract_data -> Consulta as coleções 'users', 'products' e 'carts' e retorna listas de dicionários.

import logging

logger = logging.getLogger(__name__)

def extract_data(db):
    logger.info("Iniciando Extracao...")
    users = list(db["users"].find())
    products = list(db["products"].find())
    carts = list(db["carts"].find())
    logger.info(f"Extracao: {len(users)} users, {len(products)} products, {len(carts)} carts.")
    return users, products, carts