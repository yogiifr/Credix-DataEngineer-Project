{% macro change_to_string(column) -%}
   CASE WHEN {{ column }} = 1 THEN 'Have'
   ELSE 'Dont Have' END
{%- endmacro %}
