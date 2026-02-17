# Arquivo de configuração centralizado.
# Conteúdo: Dicionário TABLE_SCHEMAS que define a ordem e o nome das colunas para cada tabela do Data Warehouse.

TABLE_SCHEMAS = {
    'dim_date': [
        'date_id', 'full_date', 'day', 'month', 'month_name', 
        'year', 'quarter', 'day_of_week', 'is_weekend'
    ],
    'dim_users': [
        'user_id', 'firstname', 'lastname', 'maidenname', 'age', 'gender', 'email', 
        'phone', 'username', 'password', 'birthdate', 'image', 'bloodgroup', 
        'height', 'weight', 'eyecolor', 'hair_color', 'hair_type', 'ip', 
        'macaddress', 'university', 'useragent', 'role', 'cpf', 'cnpj',
        'address_address', 'address_city', 'address_state', 'address_statecode', 
        'address_postalcode', 'address_country', 'address_coordinates_lat', 
        'address_coordinates_lng', 'bank_cardexpire', 'bank_cardnumber', 
        'bank_cardtype', 'bank_currency', 'bank_iban', 'company_department', 
        'company_name', 'company_title', 'company_address_address', 
        'company_address_city', 'company_address_state', 'company_address_statecode', 
        'company_address_postalcode', 'company_address_country', 
        'company_address_coordinates_lat', 'company_address_coordinates_lng', 
        'crypto_coin', 'crypto_wallet', 'crypto_network'
    ],
    'dim_products': [
        'product_id', 'title', 'description', 'category', 'price', 
        'discountpercentage', 'rating', 'stock', 'tags', 'brand', 'sku', 
        'weight', 'warrantyinformation', 'shippinginformation', 'availabilitystatus', 
        'reviews', 'returnpolicy', 'minimumorderquantity', 'images', 'thumbnail', 
        'dimensions_width', 'dimensions_height', 'dimensions_depth', 
        'meta_createdat', 'meta_updatedat', 'meta_barcode', 'meta_qrcode'
    ],
    'fact_sales': [
        'sale_id', 'user_id', 'date_id', 'total', 'discountedtotal', 
        'totalproducts', 'totalquantity', 'transaction_date'
    ],
    'fact_sales_items': [
        'item_id', 'sale_id', 'product_id', 'user_id', 'quantity', 
        'price', 'total', 'discountpercentage', 'discountedtotal'
    ]
}