-- 1. Performance Diária (Visão Geral)
SELECT 
    d.full_date,
    SUM(f.discountedtotal) as faturamento
FROM fact_sales f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;

-- 2. Top 10 Produtos por Receita
SELECT 
    p.title,
        SUM(i.discountedtotal) as receita
        FROM fact_sales_items i
        JOIN dim_products p ON i.product_id = p.product_id
        GROUP BY p.title
        ORDER BY receita DESC
        LIMIT 10;

-- 3. Faturamento por Estado
SELECT 
    u.address_state,
    SUM(f.discountedtotal) as faturamento
FROM fact_sales f
JOIN dim_users u ON f.user_id = u.user_id
GROUP BY u.address_state
ORDER BY faturamento DESC;

-- 4. Evolução do Faturamento Mensal
SELECT 
    u.address_state,
    SUM(f.discountedtotal) as faturamento
FROM fact_sales f
JOIN dim_users u ON f.user_id = u.user_id
GROUP BY u.address_state
ORDER BY faturamento DESC;

-- 3. Curva de Pareto de Produtos (Dados para o Gráfico de Pareto)
WITH product_sales AS (
    SELECT 
        p.title,
        SUM(i.discountedtotal) as receita_total
    FROM fact_sales_items i
    JOIN dim_products p ON i.product_id = p.product_id
    GROUP BY p.title
),
pareto_calc AS (
    SELECT 
        title,
        receita_total,
        SUM(receita_total) OVER (ORDER BY receita_total DESC) as receita_acumulada,
        SUM(receita_total) OVER () as receita_global
    FROM product_sales
)
SELECT 
    title,
    receita_total,
    ROUND((receita_acumulada / receita_global) * 100, 2) as porcentagem_acumulada
FROM pareto_calc
LIMIT 20;
