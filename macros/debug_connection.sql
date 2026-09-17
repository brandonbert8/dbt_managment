{% macro debug_connection() %}
  {% set sql %}
    select table_schema, table_name
    from AIRBYTE_DATABASE.information_schema.tables
    where table_schema not in ('INFORMATION_SCHEMA')
    order by 1,2
  {% endset %}
  {% set result = run_query(sql) %}
  {% if execute %}
    {% for row in result.rows %}
      {{ log('DBT_OBJECT schema=' ~ row[0] ~ ', table=' ~ row[1], info=true) }}
    {% endfor %}
  {% endif %}
{% endmacro %}
