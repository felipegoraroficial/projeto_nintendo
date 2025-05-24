SELECT COUNT(*) as total_rows 
FROM {{ ref('best-sales') }}
HAVING COUNT(*) = 0