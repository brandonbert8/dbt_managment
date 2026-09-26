{% macro generate_surrogate_key(field_list) -%}
abs(hash({%- for field in field_list -%}coalesce(cast({{ field }} as varchar), '_dbt_null_'){%- if not loop.last %}, {% endif -%}{%- endfor -%}))
{%- endmacro %}

{% macro normalize_boolean(column_name, default_value='false') -%}
case when lower(trim(to_varchar({{ column_name }}))) in ('1','true','t','yes','y','si','sí','s') then true when lower(trim(to_varchar({{ column_name }}))) in ('0','false','f','no','n') then false else {{ default_value }} end
{%- endmacro %}

{% test between(model, column_name, min_value, max_value) %}
select * from {{ model }} where {{ column_name }} is not null and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}