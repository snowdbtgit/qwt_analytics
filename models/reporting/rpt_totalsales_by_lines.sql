{{config(materialized='view', schema='reporting')}}

{% set lineos = get_line_nunmbers() %}
select

orderid,
{% for linenumber in lineos %}

sum(case when lineno={{linenumber}} then Linesalesamount end)
 as lineno{{linenumber}}_Sales,

{% endfor %}

avg(Linesalesamount) as total_Sales,
avg(margin) as avg_margin


from

{{ref('fct_orders')}}

group by 1