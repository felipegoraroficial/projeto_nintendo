%sql
SELECT *
FROM (
    SELECT *, 'Magalu' AS tabela_origem
    FROM {{source('nintendo_projeto_dev','mkt-magalu')}}
    UNION ALL
    SELECT *, 'Mercado Livre' AS tabela_origem
    FROM {{source('nintendo_projeto_dev','mkt-mercadolivre')}}
) AS big_table