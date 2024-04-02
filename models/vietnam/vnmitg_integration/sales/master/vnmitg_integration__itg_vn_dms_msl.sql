{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["msl_id","sub_channel","from_cycle","to_cycle","product_id"]
    )
}}
with source as
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_msl') }}
),
final as
(
    select 
        msl_id::varchar(10) as msl_id,
        sub_channel::varchar(50) as sub_channel,
        from_cycle::number(18,0) as from_cycle,
        to_cycle::number(18,0) as to_cycle,
        product_id::varchar(50) as product_id,
        prouct_name::varchar(100) as prouct_name,
        trim(active, ',')::varchar(5) as active,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        trim(groupmsl, ',')::varchar(100) as groupmsl
    from source
)
select * from final