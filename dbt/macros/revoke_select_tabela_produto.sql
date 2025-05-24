{% macro revoke_select_tabela_produto() %}
  REVOKE SELECT ON TABLE {{target.schema}}.`tabela-produto` TO `dbt-users`
{% endmacro %}
