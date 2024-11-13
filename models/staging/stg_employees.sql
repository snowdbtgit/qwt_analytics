{{ config(materialized = 'table' , schema = env_var('DBT_STAGESCHEMA', 'STAGING')) }}
 
select
 
empid ,
lastname ,
firstname ,
title ,
to_date(HireDate,'MM/DD/YY') as hiredate ,
office ,
extension ,
Reportsto ,
YearSalary
 
from
 
{{ source('qwt_raw', 'employees') }}