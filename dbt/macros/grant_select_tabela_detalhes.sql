{% macro grant_select_tabela_detalhes() %}
  GRANT SELECT ON TABLE {{target.schema}}.`tabela-detalhes` TO `dbt-users`
{% endmacro %}
