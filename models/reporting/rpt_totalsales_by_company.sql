{{ config(materialized='view' , schema='reporting') }}

select 

c.companyname, 
c.contactname, 
c.city, 
min(o.orderdate) as first_order_weekname ,
d.DAY_OF_WEEK_NAME as recent_order_weekname ,
sum(o.quantity) as total_orders ,
sum(o.linesalesamount) as total_sales

from

{{ref('dim_customers')}} c inner join {{ref('fct_orders')}} o 
on c.customerid = o.customerid inner join {{ref('dim_dates')}} d
on o.orderdate=d.date_day
inner join (select max(orderdate) as max_orderdate , min(orderdate) as min_orderdate from
{{ref('fct_orders')}}) oo 
on d.date_day =oo.min_orderdate

group by 
c.companyname, 
c.contactname, 
c.city, 
d.DAY_OF_WEEK_NAME,
d.DAY_OF_WEEK_NAME 