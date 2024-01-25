{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['customer_number']
    )
}}
with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_customer_hierarchy') }}
),
final as (
  select
    code::varchar(10) as customer_number,
    name::varchar(100) as customer_name,
    channel_code::varchar(50) as channel,
    customer_group_code::varchar(100) as customer_group_code,
    customer_group_name::varchar(100) as customer_group_name,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    customer_segmentation_code::varchar(256) as customer_segmentation_code,
    customer_segmentation_level_2_code::varchar(256) as customer_segmentation_level_2_code
from source
)

select * from final

