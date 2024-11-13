{{ config(materialized = 'table', schema = 'transforming' , transient= false) }}
 
select
    CustomerID,
    CompanyName,
    ContactName,
    City,
    CountrY,
    d.divisionname as divisionname,
    Address,
    Fax,
    Phone,
    PostalCode,
    case
        when StateProvince = '' then 'NA'
        when StateProvince is null then 'NA'
        when StateProvince IS NOT NULL then StateProvince
    end as StateProvince
    FROM
    {{ ref('stg_customers') }} c inner join {{ ref('lkp_divisions') }} d
    on c.DivisionID=d.DivisionID


