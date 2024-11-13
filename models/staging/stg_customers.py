def model(dbt, session):
    dbt.config(materialized='table')
    stg_Customers_df = dbt.source('qwt_raw','customers')
    return stg_Customers_df