{#
    This macro for change column day_to_year
#}

{% macro years_birth(days_birth) -%}
    ABS(CEILING(days_birth/365))
{%- endmacro %}