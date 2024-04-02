{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['branch_code', 'parent_cust_code'],
        pre_hook= "delete from {{this}} where (branch_code, parent_cust_code) in (select branch_code, parent_cust_code from {{ source('vnmsdl_raw', 'sdl_vn_interface_branch') }})"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_interface_branch') }}
),
final as(
    select 
        parent_cust_code::varchar(255) as parent_cust_code,
        parent_cust_name::varchar(255) as parent_cust_name,
        branch_code::varchar(255) as branch_code,
        branch_name::varchar(255) as branch_name,
        channel_code::varchar(255) as channel_code,
        channel_desc::varchar(255) as channel_desc,
        sales_group::varchar(255) as sales_group,
        region::varchar(255) as region,
        state::varchar(255) as state,
        city::varchar(255) as city,
        district::varchar(255) as district,
        trade_type::varchar(255) as trade_type,
        store_prioritization::varchar(255) as store_prioritization,
        latitude::number(32,16) as latitude,
        longitude::number(32,16) as longitude,
        filename::varchar(255) as filename,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final