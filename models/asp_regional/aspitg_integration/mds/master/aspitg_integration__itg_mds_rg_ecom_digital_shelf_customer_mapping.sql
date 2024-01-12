--Import CTE
with source as (
    select *
    from
        {{ source('aspsdl_raw', 'sdl_mds_rg_ecom_digital_shelf_customer_mapping') }}
),

--Logical CTE

--Final CTE
final as (
    select
        name::varchar(500) as cntry_cd,
        market::varchar(200) as market,
        channel::varchar(200) as channel,
        re::varchar(200) as re,
        group_customer::varchar(200) as group_customer,
        online_store::varchar(200) as online_store,
        data_provider::varchar(200) as data_provider,
        cluster::varchar(200) as cluster,
        current_timestamp()::timestamp_ntz(9) as ctrd_dttm
    from source
)

--Final select
select * from final

