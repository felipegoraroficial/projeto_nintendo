WITH detailstabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp
    FROM
        {{ source("nintendo_projeto", "nintendo-bigtable") }}
)

SELECT 

    id_temp AS id,
    link,
    file_date,
    status,
    origem

FROM detailstabela
ORDER BY id
