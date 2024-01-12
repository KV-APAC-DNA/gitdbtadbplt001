--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_sales_ops_map') }}
),

--Logical CTE

--Final CTE
final as (
    select
        dataset::varchar(200) as dataset,
        source_cluster::varchar(200) as source_cluster,
        destination_cluster::varchar(200) as destination_cluster,
        source_market::varchar(200) as source_market,
        destination_market::varchar(200) as destination_market,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)

--Final select
select * from final
