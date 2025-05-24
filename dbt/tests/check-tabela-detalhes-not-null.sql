SELECT COUNT(*) as total_rows 
FROM {{ ref('tabela-detalhes') }}
HAVING COUNT(*) = 0