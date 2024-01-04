
{% macro fill_null(occupation_type) -%}
   CASE WHEN  occupation_type = '' THEN 'Others'
   ELSE occupation_type
   END
{%- endmacro %}