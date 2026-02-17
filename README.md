# Magazord Data Engineer Challenge - ETL Pipeline

Este repositório contém a solução desenvolvida para o desafio técnico de Engenharia de Dados da **Magazord**. O objetivo principal é a construção de um pipeline de dados robusto que migra dados de uma origem NoSQL (**MongoDB**) para um Data Warehouse relacional (**PostgreSQL**) modelado para fins analíticos.

## 📁 Estrutura de Pastas

```text
├── src/
│   ├── database.py    # Definição de DDL e Schema
│   ├── extract.py     # Leitura de coleções MongoDB
│   ├── transform.py   # Regras de negócio e Star Schema
│   ├── load.py        # Lógica de Upsert e carga SQL
│   ├── config.py      # Configurações de tabelas e mapeamentos
│   └── utils.py       # Funções auxiliares e conversores
├── main.py            # Orquestrador central do ETL
├── docker-compose.yml # Infraestrutura como código
└── requirements.txt   # Dependências do projeto

## 🏗️ Arquitetura e Tecnologias

O projeto foi estruturado para garantir escalabilidade e facilidade de manutenção:

* **Linguagem:** Python 3.10
* **Processamento de Dados:** Pandas (utilizado para transformação e normalização)
* **Banco de Dados de Origem:** MongoDB 8.0 (Raw Data)
* **Banco de Dados de Destino:** PostgreSQL 17 (Data Warehouse)
* **Orquestração:** Docker & Docker Compose
* **Comunicação DB:** SQLAlchemy & Psycopg2

## 📊 Modelagem de Dados (Star Schema)

Para suportar consultas analíticas de alto desempenho, os dados foram transformados em um modelo **Estrela (Star Schema)**:

* **Fatos:**
    * `fact_sales`: Consolidação de cabeçalhos de pedidos, totais e datas.
    * `fact_sales_items`: Granularidade ao nível de item/SKU para análises de mix de produtos.
* **Dimensões:**
    * `dim_users`: Atributos demográficos e geográficos dos clientes.
    * `dim_products`: Informações detalhadas sobre o catálogo de produtos.
    * `dim_date`: Dimensão de tempo gerada para facilitar filtros temporais (ano, mês, dia da semana, trimestres).

---

## 🛠️ Decisões de Engenharia e Boas Práticas

### 1. Idempotência (Upsert Logic)
O pipeline utiliza uma estratégia de **Upsert** baseada em tabelas temporárias. Antes da carga final, os dados são inseridos em uma `temp_table` e movidos para a tabela definitiva utilizando `ON CONFLICT (pk) DO UPDATE`. Isso garante que, se o script rodar múltiplas vezes, o estado do banco permaneça consistente sem duplicatas.

### 2. Tratamento de Dados e Resiliência
* **Datas Híbridas:** Implementação da função `converter_data_hibrida` que lida automaticamente com formatos variados (Unix Timestamp e strings ISO) encontrados no MongoDB.
* **Schema Enforcement:** Uso de um dicionário de mapeamento (`TABLE_SCHEMAS`) para garantir que o DataFrame final possua exatamente as colunas e tipos esperados pelo PostgreSQL.
* **Limpeza de Tipos Complexos:** Conversão automática de dicionários e listas aninhadas do JSON original em strings/objetos compatíveis com SQL.

### 3. Observabilidade
O pipeline utiliza o módulo `logging` do Python para fornecer visibilidade sobre cada etapa do processo:
* `INFO`: Registra o início/fim de cada etapa e volumetria processada.
* `WARNING`: Alerta sobre datas inválidas ou inconsistências menores que foram tratadas.
* `ERROR`: Reporta falhas críticas de conexão ou esquema.

---

## 🚀 Como Executar

**Pré-requisitos:** Ter o [Docker](https://www.docker.com/) e o Docker Compose instalados.

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/seu-usuario/data_engineer_test.git](https://github.com/seu-usuario/data_engineer_test.git)
    cd data_engineer_test
    ```

2.  **Inicie o ambiente:**
    ```bash
    docker-compose up -d --build
    ```
    *Este comando subirá o MongoDB, o PostgreSQL e disparará automaticamente o container `etl_job` que processa os dados.*

3.  **Acompanhe o processamento:**
    ```bash
    docker logs -f etl_job
    ```

---

## 🔍 Análises Disponíveis

Dentro do repositório, você encontrará:

* **`queries.sql`**: Consultas otimizadas para o DW, incluindo:
    * Faturamento mensal e acumulado.
    * Análise de Pareto de produtos.
    * Ticket médio por estado brasileiro.
* **`analises.ipynb`**: Notebook com visualizações estatísticas (Boxplots) sobre o comportamento de compra por faixa etária, facilitando a identificação de outliers e padrões de consumo.

---



## 📈 Insights e Visualizações

As análises foram geradas processando os dados do Data Warehouse e exportadas via Jupyter Notebook.

### Distribuição de Vendas por Faixa Etária
O gráfico abaixo permite identificar o comportamento de compra e o ticket médio de cada grupo demográfico, auxiliando na segmentação de campanhas de marketing.

![Boxplot Idade](img/boxplot_idade.png)

### Curva de Pareto (Produtos)
Análise de concentração de receita (Regra 80/20), identificando quais produtos representam a maior parte do faturamento da plataforma.

![Pareto Chart](img/pareto_chart.png)