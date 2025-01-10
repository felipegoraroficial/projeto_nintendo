-- Etapa 1: Criação das CTEs (Common Table Expressions)
WITH produtos AS (
    SELECT
        id,
        titulo,
        memoria,
        oled,
        lite
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
        produtos.memoria,
        produtos.oled,
        produtos.lite,
        preco.preco_promo,
        detalhes.origem
    FROM produtos
    JOIN preco ON produtos.id = preco.id
    JOIN detalhes ON produtos.id = detalhes.id
    WHERE detalhes.origem IN ('mercadolivre', 'magalu') -- Filtra as origens "mercadolivre" e "magalu"
),

-- Etapa 3: Seleção do menor preço para "mercadolivre"
menor_preco_mercadolivre AS (
    SELECT
        first_value(id) OVER (partition by origem ORDER BY preco_promo) AS id,
        first_value(preco_promo) OVER (partition by origem ORDER BY preco_promo) AS menor_preco,
        'mercadolivre' AS origem
    FROM produtos_filtrados
    WHERE origem = 'mercadolivre'
    ORDER BY preco_promo
    LIMIT 1
),

-- Etapa 4: Seleção do menor preço para "magalu"
menor_preco_magalu AS (
    SELECT
        first_value(id) OVER (partition by origem ORDER BY preco_promo) AS id,
        first_value(preco_promo) OVER (partition by origem ORDER BY preco_promo) AS menor_preco,
        'magalu' AS origem
    FROM produtos_filtrados
    WHERE origem = 'magalu'
    ORDER BY preco_promo
    LIMIT 1
)

-- Etapa 5: União dos resultados
SELECT * FROM menor_preco_mercadolivre
UNION ALL
SELECT * FROM menor_preco_magalu



