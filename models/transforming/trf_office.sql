{{ config(materialized = 'table', schema = 'transforming') }}
 
select
Office,
OfficeAddress,
OfficePostalCode,
OfficeCity,
case
        when OfficeStateProvince = '' then 'NA'
        when OfficeStateProvince is null then 'NA'
        when OfficeStateProvince IS NOT NULL then OfficeStateProvince
    end as OfficeStateProvince,
OfficePhone,
OfficeFax,
OfficeCountry
    FROM
    {{ ref('stg_office') }}


