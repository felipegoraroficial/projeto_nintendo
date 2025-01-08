WITH pricetabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp
    FROM
        {{ source("nintendo_projeto_prd", "nintendo-bigtable") }}
)

SELECT 

    id_temp AS id,
    moeda,
    condition_promo,
    preco_promo,
    parcelado

FROM pricetabela
ORDER BY preco_promo ASC
