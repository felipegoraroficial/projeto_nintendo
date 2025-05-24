{% macro grant_usage_schema() %}
  GRANT SELECT ON SCHEMA {{target.schema}} TO `dbt-users`
{% endmacro %}
