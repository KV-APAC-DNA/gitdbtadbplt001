{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["code"]
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_naver_product_master') }}
),
final as 
(
    select
        trim(code)::varchar(100) as code,
        trim(category_l_code)::varchar(100) as category_l_name,
        trim(category_m_code)::varchar(100) as category_m_name,
        trim(category_s_code)::varchar(100) as category_s_name,
        trim(brands_name)::varchar(100) as brands_name,
        trim(product_name)::varchar(1000) as product_name,
        lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
        current_timestamp()::timestamp_ntz(9) as refresh_date
    from source
)
select * from final