WITH producttabela AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY link) AS id_temp
    FROM
        {{ source("nintendo_projeto_dev", "nintendo-bigtable") }}
)

SELECT 

    id_temp AS id,
    titulo,
    memoria,
    oled,
    lite

FROM producttabela
ORDER BY id


