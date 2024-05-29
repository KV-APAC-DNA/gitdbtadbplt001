{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["sub_customer_code", "customer_code"]
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_sub_customer_master') }}
),
final as
(
    select
        'KR'::varchar(2) as cntry_cd,
        name::varchar(100) as sub_customer_name,
        code::varchar(50) as sub_customer_code,
        customer_code::varchar(50) as customer_code,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as created_dt
    from source
    where upper(trim(retailer_code)) = 'DEPT & DAISO'
)
select * from final