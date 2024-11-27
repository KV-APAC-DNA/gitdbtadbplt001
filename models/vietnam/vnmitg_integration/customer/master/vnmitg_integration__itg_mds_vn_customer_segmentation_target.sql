{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['shoptype_code','year_code','month_code']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_customer_segmentation_target') }}
),

final as
(
    select
    shoptype_code::varchar(500) as shoptype_code,
shoptype_name::varchar(600) as shoptype_name,
shoptype_id::number(10,0) as shoptype_id,
subchannel_code::varchar(500) as subchannel_code,
subchannel_name::varchar(500) as subchannel_name,
subchannel_id::number(10,0) as subchannel_id,
re::varchar(200) as re,
channel_code::varchar(500) as channel_code,
channel_name::varchar(500) as channel_name,
channel_id::number(10,0) as channel_id,
re_segmentation_code::varchar(500) as re_segmentation_code,
re_segmentation_name::varchar(500) as re_segmentation_name,
re_segmentation_id::number(10,0) as re_segmentation_id,
year_code::varchar(500) as year_code,
year_name::varchar(500) as year_name,
year_id::number(10,0) as year_id,
month_code::varchar(500) as month_code,
month_name::varchar(500) as month_name,
month_id::number(10,0) as month_id,
target::number(38,0) as target,
current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final