{% macro grant_select_tabela_preco() %}
  GRANT SELECT ON TABLE {{target.schema}}.`tabela-preco` TO `dbt-users`
{% endmacro %}
