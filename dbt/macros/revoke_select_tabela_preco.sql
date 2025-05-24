{% macro revoke_select_tabela_preco() %}
  REVOKE SELECT ON TABLE {{target.schema}}.`tabela-preco` TO `dbt-users`
{% endmacro %}
