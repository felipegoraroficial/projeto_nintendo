{% macro revoke_select_tabela_detalhes() %}
  REVOKE SELECT ON TABLE {{target.schema}}.`tabela-detalhes` TO `dbt-users`
{% endmacro %}
