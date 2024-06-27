{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["customer_code"]
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_customer_hierarchy') }}
),
final as 
(
    select 
        code::varchar(500) as customer_code,
        name::varchar(500) as customer_name,
        parent_customer::varchar(200) as parent_customer,
        local_customer_segmentation_level_1_code::varchar(500) as local_customer_segmentation_1,
        local_customer_segmentation_level_2_code::varchar(500) as local_customer_segmentation_2,
        channel_level_1_code::varchar(500) as channel1,
        channel_level_2_code::varchar(500) as channel2,
        channel_level_3_code::varchar(500) as channel3
    from source
)
select * from final