{{
    config(
        materialized = 'table',
    )
}}

SELECT 
    codigo AS id,    -- Seleciona a coluna 'codigo'
    titulo -- Seleciona a coluna 'titulo'


FROM {{ source("nintendo_projeto", "nintendo-bigtable") }} -- Fonte de dados: tabela "nintendo-bigtable" no projeto "nintendo_projeto"
WHERE status = "ativo"
ORDER BY id        -- Ordena os resultados pela coluna 'id'
