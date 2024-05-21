{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["prft_ctr"]
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_product_hierarchy') }}
),
final as
(
    select 
        code::varchar(500) as prft_ctr,
        local_brand_classification_code::varchar(200) as local_brand_classification_code,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from source