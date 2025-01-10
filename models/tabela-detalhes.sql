-- Etapa 1: Criação da CTE (Common Table Expression) chamada 'detailstabela'
WITH detailstabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp -- Cria uma coluna id_temp com numeração sequencial baseada no campo 'link'
    FROM
        {{ source("nintendo_projeto_dev", "nintendo-bigtable") }} -- Fonte de dados: tabela "nintendo-bigtable" no projeto "nintendo_projeto_dev"
)

-- Etapa 2: Seleção dos dados da CTE 'detailstabela'
SELECT 
    id_temp AS id,   -- Renomeia a coluna 'id_temp' para 'id'
    link,            -- Seleciona a coluna 'link'
    file_date,       -- Seleciona a coluna 'file_date'
    status,          -- Seleciona a coluna 'status'
    origem           -- Seleciona a coluna 'origem'

FROM detailstabela -- Origem dos dados: CTE 'detailstabela'
ORDER BY id        -- Ordena os resultados pela coluna 'id'

