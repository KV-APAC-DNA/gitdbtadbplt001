{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['dstrbtr_id','promotion_id'],
        pre_hook= "{%if is_incremental() %}
        delete from {{this}} where (dstrbtr_id, promotion_id) in ( select dstrbtr_id, promotion_id from {{ source('vnmsdl_raw', 'sdl_vn_dms_promotion_list') }} where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__duplicate_test')}} )
        ); {% endif %}"
    )
}}

with source as(
    select *, dense_rank() over (partition by dstrbtr_id,promotion_id order by file_name desc) rnk
     from {{ source('vnmsdl_raw', 'sdl_vn_dms_promotion_list') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_promotion_list__duplicate_test')}}
        ) qualify rnk = 1
),
final as(
    select
        dstrbtr_id::varchar(30) as dstrbtr_id,
        cntry_code::varchar(2) as cntry_code,
        promotion_id::varchar(50) as promotion_id,
        promotion_name::varchar(256) as promotion_name,
        promotion_desc::varchar(256) as promotion_desc,
        try_to_date(start_date, 'MM/DD/YYYY HH12:MI:SS AM') as start_date,
        try_to_date(end_date, 'MM/DD/YYYY HH12:MI:SS AM') as end_date,
        status::varchar(1) as status,
        curr_date as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)
select * from final