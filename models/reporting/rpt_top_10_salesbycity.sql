{{config(materialized='view', schema='reporting')}}

SELECT 
c.COMPANYNAME, 
c.CONTACTNAME,
SUM(o.LINESALESAMOUNT) as LINESALESAMOUNT, 
SUM(o.QUANTITY) as TOTAL_ORDERS, 
AVG(O.margin) as AVG_MARGIN

FROM 

{{ ref('dim_customers') }} c inner join {{ ref('fct_orders') }} o
on c.customerid = o.customerid

WHERE c.CITY in ({{ var('v_city') }})
GROUP BY 1, 2
ORDER BY LINESALESAMOUNT DESC