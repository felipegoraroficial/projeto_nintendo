-- Etapa 1: Criação da CTE (Common Table Expression) chamada 'pricetabela'
WITH pricetabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp -- Cria uma coluna id_temp com numeração sequencial baseada no campo 'link'
    FROM
        {{ source("nintendo_projeto_prd", "nintendo-bigtable") }} -- Fonte de dados: tabela "nintendo-bigtable" no projeto "nintendo_projeto_prd"
)

-- Etapa 2: Seleção dos dados da CTE 'pricetabela'
SELECT 
    id_temp AS id, -- Renomeia a coluna 'id_temp' para 'id'
    moeda,         -- Seleciona a coluna 'moeda'
    condition_promo, -- Seleciona a coluna 'condition_promo'
    preco_promo,     -- Seleciona a coluna 'preco_promo'
    parcelado        -- Seleciona a coluna 'parcelado'

FROM pricetabela -- Origem dos dados: CTE 'pricetabela'
ORDER BY preco_promo ASC -- Ordena os resultados pela coluna 'preco_promo' em ordem ascendente

