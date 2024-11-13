{{ config(materialized='view' , schema='reporting') }}
with cust as
(
select customerid,companyname,contactname,city
from {{ref('dim_customers')}} 
),
od as
(
select customerid,orderdate , quantity,linesalesamount
from {{ref('fct_orders')}} 
),
cust_orders as
(
select 
cust.companyname as company_name, 
cust.contactname as contact_name, 
cust.city as city, 
min(od.orderdate) as first_order_date,
max(od.orderdate) as recent_order_date,
sum(od.quantity) as total_orders ,
sum(od.linesalesamount) as total_sales
from cust inner join od on cust.customerid = od.customerid 
group by 
company_name, 
contact_name, 
city
order by total_sales desc
)
select * from cust_orders