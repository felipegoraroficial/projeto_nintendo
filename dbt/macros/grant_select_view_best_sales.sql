{% macro grant_select_view_best_sales() %}
  GRANT SELECT ON VIEW {{target.schema}}.`best-sales` TO `dbt-users`
{% endmacro %}
