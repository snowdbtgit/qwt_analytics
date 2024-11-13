{{ config(materialized='table' , schema = env_var('DBT_STAGESCHEMA', 'STAGING') ) }} 

select 

Office	as officeid,
OfficeAddress as address,
OfficePostalCode as PostalCode,
OfficeCity as city,	
OfficeStateProvince	as StateProvince,
OfficePhone	as Phone,
OfficeFax as fax ,
OfficeCountry as country

from 

{{ source("qwt_raw", "offices") }}
