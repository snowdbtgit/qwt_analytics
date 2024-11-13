{{ config(materialized='table', schema='transforming') }}

select

shipments.orderid,
shipments.lineno,
shippers.companyname,
shipments.shipmentdate,
shipments.status,
shipments.dbt_valid_from,
shipments.dbt_valid_to

from

{{ref('shipments_snapshot')}} shipments inner join {{ref('lkp_shippers')}} shippers
on shipments.shipperid=shippers.shipperid

