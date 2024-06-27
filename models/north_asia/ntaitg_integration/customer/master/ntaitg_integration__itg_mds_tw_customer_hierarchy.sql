{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["customer_strategic_typeb"]
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_customer_hierarchy') }}
),
final as 
(
    select 
        code::varchar(500) as customer_strategic_typeb,
        name::varchar(500) as name,
        local_customer_segmentation_level_1_code::varchar(500) as local_customer_segmentation_1,
        local_customer_segmentation_level_2_code::varchar(500) as local_customer_segmentation_2,
        channel_level_1_code::varchar(500) as channel1,
        sales_office_code::varchar(500) as sales_office_code,
        sales_group_code::varchar(500) as sales_group_code
    from source
)
select * from final