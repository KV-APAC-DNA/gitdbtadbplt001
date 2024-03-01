{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['sold_to']
    )
}}

with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_customer_hierarchy') }}
),
final as (
select
    channel_code::varchar(200) as channel,
    re_code::varchar(200) as re,
    sub_re_code::varchar(200) as sub_re,
    region_code::varchar(200) as region,
    code::varchar(500) as sold_to,
    customer_name_code::varchar(200) as customer_name,
    segmentation_code::varchar(500) as segmentation,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    customer_segmentation_level_2_code::varchar(256) as customer_segmentation_level_2_code
from source
)

select * from final