SELECT count(*) count, id, origem FROM 
{{ref('best-sales')}}
group by id,origem
HAVING COUNT > 1