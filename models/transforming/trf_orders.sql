{{ config(materialized='table' , schema ='transforming')}}

select 

o.orderid,
od.lineno,
o.orderdate,
o.customerid,
o.employeeid,
o.shipperid,
od.productid,
od.quantity,
od.unitprice,
od.discount,
(od.UnitPrice * od.Quantity) * (1-od.Discount) as Linesalesamount,
p.UnitCost * od.Quantity as CostOfGoodsSold,
((od.UnitPrice * od.Quantity) * (1-od.Discount)) - (p.UnitCost * od.Quantity) as margin

from

{{ref('stg_orders')}} o inner join {{ref('stg_orderdetails')}} od on
o.orderid=od.orderid inner join {{ref('stg_products')}} p on
p.productid=od.productid