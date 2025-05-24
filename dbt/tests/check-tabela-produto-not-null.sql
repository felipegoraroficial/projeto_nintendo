SELECT COUNT(*) as total_rows 
FROM {{ ref('tabela-produto') }}
HAVING COUNT(*) = 0