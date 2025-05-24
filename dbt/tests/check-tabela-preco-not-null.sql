SELECT COUNT(*) as total_rows 
FROM {{ ref('tabela-preco') }}
HAVING COUNT(*) = 0