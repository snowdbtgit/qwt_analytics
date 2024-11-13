{{config(materialized='incremental' , unique_key= ['OrderID' , 'lineno'],schema = env_var('DBT_STAGESCHEMA', 'STAGING'))}} 

select 

od.*,
o.orderdate as orderdate
 
from 

{{source('qwt_raw','orders')}} o inner join

{{source('qwt_raw','orderdetails')}} od

on o.OrderID=od.OrderID

{% if is_incremental() %}

--this filter will only be applied for an incremntal run
where orderdate >(select max(orderdate) from {{this}})

{% endif %}

