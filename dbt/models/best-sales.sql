{{
    config(
        materialized = 'view',
    )
}}

-- Etapa 1: Criação das CTEs (Common Table Expressions)
WITH produtos AS (
    SELECT
        id,
        titulo

    FROM {{ ref('tabela-produto') }} -- Fonte de dados: tabela "tabela-produto"
),
preco AS (
    SELECT
        id,
        preco_promo
    FROM {{ ref('tabela-preco') }} -- Fonte de dados: tabela "tabela-preco"
),
detalhes AS (
    SELECT
        id,
        origem
    FROM {{ ref('tabela-detalhes') }} -- Fonte de dados: tabela "tabela-detalhes"
),

-- Etapa 2: Filtragem e junção das CTEs
produtos_filtrados AS (
    SELECT
        produtos.id,
        produtos.titulo,
        preco.preco_promo,
        detalhes.origem
    FROM produtos
    JOIN preco ON produtos.id = preco.id
    JOIN detalhes ON produtos.id = detalhes.id
    WHERE detalhes.origem IN ('mercadolivre', 'magalu') -- Filtra as origens "mercadolivre" e "magalu"
),

-- Etapa 3: Obtenção do menor preço por origem
menores_precos AS (
    SELECT
        id,
        preco_promo AS menor_preco,
        origem
    FROM produtos_filtrados
    WHERE origem IN ('mercadolivre', 'magalu')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY origem ORDER BY preco_promo ASC) = 1
)

-- Etapa 5: União dos resultados
SELECT * FROM menores_precos