SELECT *
FROM {{ source("nintendo_projeto_prd", "nintendo-bigtable") }}
WHERE file_name LIKE '%mercadolivre%'
ORDER BY preco_promo ASC