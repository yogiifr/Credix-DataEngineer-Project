{#
    This macro for change column day_to_year
#}

{% macro years_employed(days_employed) -%}
   CASE WHEN  days_employed = 365243 THEN 50
   ELSE ABS(CEILING(days_employed/365)) END
{%- endmacro %}