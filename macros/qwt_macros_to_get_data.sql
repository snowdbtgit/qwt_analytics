{% macro get_line_nunmbers() -%}
 
{% set line_numbers_query %}
select distinct
lineno
from {{ ref('fct_orders') }}
order by 1
{% endset %}
 
{% set results = run_query(line_numbers_query) %}
 
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}
 
{{ return(results_list) }}
 
{%- endmacro %}