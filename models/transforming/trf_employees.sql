{{ config(materialized='table' , schema= 'transforming') }}

select 

e.EmpID ,	
e.LastName ,	
e.FirstName ,
e.Title 	,
e.hiredate ,	
o.city,
o.address,
o.phone,
iff(e.Extension = '-' , 'NA' , e.Extension) as extension,
iff(m.title is null, e.title, m.title) as manager_title,	
iff(m.FirstName is null , e.FirstName , m.FirstName) as manager_nm	,
e.YearSalary 

from 

{{ ref('stg_employees') }} e inner join {{ ref('stg_office') }} o
on e.office=o.officeid
inner join {{ ref('stg_employees') }} m
on e.ReportsTo=m.EmpID
