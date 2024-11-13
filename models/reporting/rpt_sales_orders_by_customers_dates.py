import snowflake.snowpark.functions as F

def model(dbt,session):

    dbt.config(materialized= 'table' , schema= 'reporting')

    dim_customers_df = dbt.ref('dim_customers')

    fct_orders_df = dbt.ref('fct_orders')

    customers_orders_df = (fct_orders_df.group_by ('customerid')
                      .agg
                         (
                          F.min(F.col('orderdate')).alias ('first_order'),
                          F.max(F.col('orderdate')).alias ('most_recent_order'),
                          F.sum(F.col('quantity')).alias ('total_orders'),
                          F.sum(F.col('linesalesamount')).alias ('total_Sales')
                          )
                        )

    final_df = (
            dim_customers_df 
            .join(customers_orders_df, dim_customers_df.customerid == customers_orders_df.customerid , 'left')
            .select(
                  dim_customers_df.companyname.alias('companyname'),
                dim_customers_df.contactname.alias('contactname'),
                dim_customers_df.city.alias('city'),
                customers_orders_df.first_order.alias('first_order'),
                customers_orders_df.most_recent_order.alias('most_recent_order'),
                customers_orders_df.total_orders.alias('total_orders'),
                customers_orders_df.total_sales.alias('total_sales')
            )

)

    return final_df