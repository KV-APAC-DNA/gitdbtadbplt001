{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["code"]
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sub_customer_master') }}
),
final as (
    select
        code::varchar(500) as code,
        name::varchar(500) as name,
        sap_customer_code::varchar(200) as sap_customer_code,
        retailer::varchar(200) as retailer,
        outlet_code::varchar(200) as outlet_code,
        pharmacy_name::varchar(200) as pharmacy_name,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final