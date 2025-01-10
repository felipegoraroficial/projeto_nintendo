-- Etapa 1: Criação da CTE (Common Table Expression) chamada 'producttabela'
WITH producttabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp -- Cria uma coluna id_temp com numeração sequencial baseada no campo 'link'
    FROM
        {{ source("nintendo_projeto_prd", "nintendo-bigtable") }} -- Fonte de dados: tabela "nintendo-bigtable" no projeto "nintendo_projeto_prd"
)

-- Etapa 2: Seleção dos dados da CTE 'producttabela'
SELECT 
    id_temp AS id, -- Renomeia a coluna 'id_temp' para 'id'
    titulo,        -- Seleciona a coluna 'titulo'
    memoria,       -- Seleciona a coluna 'memoria'
    oled,          -- Seleciona a coluna 'oled'
    lite           -- Seleciona a coluna 'lite'

FROM producttabela -- Origem dos dados: CTE 'producttabela'
ORDER BY id        -- Ordena os resultados pela coluna 'id'



