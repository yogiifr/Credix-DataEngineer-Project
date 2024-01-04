{% macro change_to_int(column) -%}
   CASE WHEN {{ column }} = 'Y' THEN 1
   ELSE 0 END
{%- endmacro %}
