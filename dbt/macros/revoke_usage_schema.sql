{% macro revoke_usage_schema() %}
  REVOKE SELECT ON SCHEMA {{target.schema}} TO `dbt-users`
{% endmacro %}
