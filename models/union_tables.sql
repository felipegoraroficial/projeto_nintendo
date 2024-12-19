
select *
from
    (
        select *, 'Magalu' as tabela_origem
        from {{ source("nintendo_projeto_dev", "mkt-magalu") }}
        union all
        select *, 'Mercado Livre' as tabela_origem
        from {{ source("nintendo_projeto_dev", "mkt-mercadolivre") }}
    ) as big_table
