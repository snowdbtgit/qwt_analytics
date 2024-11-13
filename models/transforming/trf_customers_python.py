def model(dbt, session):
    trf_Customers_df = dbt.ref('stg_customers')
    return trf_Customers_df