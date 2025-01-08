with produtos as (
    select
        id,
        titulo,
        memoria,
        oled,
        lite
    from {{ ref('tabela-produto') }}
),
preco as (
    select
        id,
        preco_promo
    from {{ ref('tabela-preco') }}
),
detalhes as (
    select
        id,
        origem
    from {{ ref('tabela-detalhes') }}
),
produtos_filtrados as (
    select
        produtos.id,
        produtos.titulo,
        produtos.memoria,
        produtos.oled,
        produtos.lite,
        preco.preco_promo,
        detalhes.origem
    from produtos
    join preco on produtos.id = preco.id
    join detalhes on produtos.id = detalhes.id
    where detalhes.origem in ('mercadolivre', 'magalu')
),
menor_preco_mercadolivre as (
    select
        first_value(id) over (partition by origem order by preco_promo) as id,
        first_value(preco_promo) over (partition by origem order by preco_promo) as menor_preco,
        'mercadolivre' as origem
    from produtos_filtrados
    where origem = 'mercadolivre'
    order by preco_promo
    limit 1
),
menor_preco_magalu as (
    select
        first_value(id) over (partition by origem order by preco_promo) as id,
        first_value(preco_promo) over (partition by origem order by preco_promo) as menor_preco,
        'magalu' as origem
    from produtos_filtrados
    where origem = 'magalu'
    order by preco_promo
    limit 1
)

select * from menor_preco_mercadolivre
union all
select * from menor_preco_magalu


