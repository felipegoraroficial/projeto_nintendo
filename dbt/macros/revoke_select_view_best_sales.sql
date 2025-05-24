{% macro revoke_select_view_best_sales() %}
  REVOKE SELECT ON VIEW {{target.schema}}.`best-sales` TO `dbt-users`
{% endmacro %}
