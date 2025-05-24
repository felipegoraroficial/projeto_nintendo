{% macro grant_select_tabela_produto() %}
  GRANT SELECT ON TABLE {{target.schema}}.`tabela-produto` TO `dbt-users`
{% endmacro %}
