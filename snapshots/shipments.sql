{% snapshot shipments_snapshot %}

{{
config
(
    target_database ='qwt_analytics',
    target_schema = 'snapshots',
    unique_key = "orderid||'-'||lineno",

    strategy ='timestamp',
    updated_at ='shipmentdate',

)

}}

select * from {{ source('qwt_raw' , 'shipments')}}

{% endsnapshot %}