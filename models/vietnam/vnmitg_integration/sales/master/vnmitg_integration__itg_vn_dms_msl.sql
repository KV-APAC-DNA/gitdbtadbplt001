{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["msl_id","sub_channel","from_cycle","to_cycle","product_id"]
    )
}}
with source as
(
    select *, dense_rank() over (partition by msl_id,sub_channel,from_cycle,to_cycle,product_id order by file_name desc) rnk
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_msl') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_msl__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_msl__duplicate_test')}}
    ) qualify rnk = 1
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
        trim(groupmsl, ',')::varchar(100) as groupmsl,
        file_name::varchar(255) as file_name 
    from source
)
select * from final