# src/transform.py

# Módulo que aplica a modelagem Star Schema (OLAP).
# Funções: 
# - transform_users: Normaliza dados de usuários e tipos numéricos.
# - transform_products: Limpa catálogo e preenche valores nulos em marcas.
# - transform_sales: Faz o "explode" dos carrinhos em tabelas de Fato (Vendas e Itens).
# - transform_dim_date: Gera a dimensão de tempo a partir das datas das vendas.

import pandas as pd
from src.utils import converter_data_hibrida

def transform_users(users_raw):
    df = pd.json_normalize(users_raw).drop(columns=['_id'], errors='ignore')
    df.columns = [c.replace('.', '_').lower() for c in df.columns]
    
    df = df.dropna(subset=['id']).rename(columns={'id': 'user_id'})
    df['user_id'] = df['user_id'].astype(int)
    
    cols_numeric = ['height', 'weight', 'address_coordinates_lat', 'address_coordinates_lng', 
                    'company_address_coordinates_lat', 'company_address_coordinates_lng']
    for col in cols_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    df = df.drop_duplicates(subset=['user_id'])
    return df

def transform_products(products_raw):
    df = pd.json_normalize(products_raw).drop(columns=['_id'], errors='ignore')
    df.columns = [c.replace('.', '_').lower() for c in df.columns]
    
    df = df.dropna(subset=['id']).rename(columns={'id': 'product_id'})
    df['product_id'] = df['product_id'].astype(int)
    df['brand'] = df['brand'].fillna('Nao Informado').replace('', 'Nao Informado')
    
    for col in ['meta_createdat', 'meta_updatedat']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df = df.drop_duplicates(subset=['product_id'])
    return df

def transform_dim_date(fact_sales_df):
    if fact_sales_df.empty: return pd.DataFrame()
    min_date = fact_sales_df['transaction_date'].min()
    max_date = fact_sales_df['transaction_date'].max()
    if pd.isna(min_date) or pd.isna(max_date): return pd.DataFrame()

    date_range = pd.date_range(start=min_date.date(), end=max_date.date())
    dim_date = pd.DataFrame({'full_date': date_range})
    dim_date['date_id'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['day_of_week'] = dim_date['full_date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['full_date'].dt.dayofweek.isin([5, 6])
    return dim_date

def transform_sales(carts_raw):
    df_c = pd.DataFrame(carts_raw).drop(columns=['_id'], errors='ignore')
    df_c = df_c.drop_duplicates(subset=['id'])

    # Fact Sales
    f_sales = df_c.drop(columns=['products']).rename(columns={'id': 'sale_id', 'userId': 'user_id'})
    f_sales['transaction_date'] = converter_data_hibrida(f_sales['transaction_date'], "fact_sales")
    f_sales = f_sales.dropna(subset=['sale_id', 'user_id', 'transaction_date'])
    f_sales['date_id'] = f_sales['transaction_date'].dt.strftime('%Y%m%d').astype(int)
    f_sales['sale_id'] = f_sales['sale_id'].astype(int)
    f_sales['user_id'] = f_sales['user_id'].astype(int)
    f_sales.columns = f_sales.columns.str.lower()
    f_sales = f_sales.drop_duplicates(subset=['sale_id'])

    # Fact Sales Items
    df_exp = df_c.explode('products')
    items_norm = pd.json_normalize(df_exp['products'])
    f_items_raw = pd.concat([
        df_exp[['id', 'userId']].rename(columns={'id': 'sale_id', 'userId': 'user_id'}).reset_index(drop=True),
        items_norm.rename(columns={'id': 'product_id'}).reset_index(drop=True)
    ], axis=1)
    
    f_items_raw = f_items_raw.dropna(subset=['sale_id', 'product_id'])
    f_items = f_items_raw.groupby(['sale_id', 'product_id', 'user_id']).agg({
        'quantity': 'sum', 'price': 'mean', 'total': 'sum', 
        'discountPercentage': 'mean', 'discountedTotal': 'sum'
    }).reset_index()

    f_items['item_id'] = f_items['sale_id'].astype(str) + "_" + f_items['product_id'].astype(str)
    f_items['sale_id'] = f_items['sale_id'].astype(int)
    f_items['product_id'] = f_items['product_id'].astype(int)
    f_items['user_id'] = f_items['user_id'].astype(int)
    f_items.columns = f_items.columns.str.lower()
    
    return f_sales, f_items