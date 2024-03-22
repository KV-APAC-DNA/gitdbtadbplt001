{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['sold_to']
    )
}}

with source as (
    select * from {{ source('myssdl_raw','sdl_mds_my_customer_hierarchy') }}
  
),

final as (
    select
        code::varchar(10) as sold_to,
        name::varchar(200) as sold_to_desc,
        cust_grp_code_code::varchar(10) as cust_grp_code,
        cust_grp_code_name::varchar(50) as cust_grp,
        customer::varchar(200) as customer,
        channel_code::varchar(50) as channel,
        channel_name::varchar(100) as channel_name,
        territory_code::varchar(50) as territory,
        territory_name::varchar(100) as territory_name,
        reg_channel_code::varchar(50) as reg_channel,
        reg_channel_name::varchar(100) as reg_channel_name,
        reg_group_code::varchar(50) as reg_group,
        reg_group_name::varchar(100) as reg_group_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        customer_segmentation_level_2_code::varchar(256) as customer_segmentation_level_2_code,
        reg_group1_code::varchar(256) as reg_group1
    from source
    )
select * from final
