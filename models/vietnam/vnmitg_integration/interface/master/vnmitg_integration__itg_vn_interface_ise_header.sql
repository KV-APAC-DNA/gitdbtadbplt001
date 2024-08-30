{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['ise_id', 'ise_desc'],
        pre_hook= "{% if is_incremental()%} 
        delete from {{this}} where (ise_id, ise_desc) in ( select ise_id, ise_desc from {{ source('vnmsdl_raw', 'sdl_vn_interface_ise_header') }} where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_ise_header__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_ise_header__duplicate_test')}}
        ));
        {%endif%}"
    )
}}

with source as(
    select *,dense_rank() over (partition by ise_id,ise_desc order by filename desc) rnk
     from {{ source('vnmsdl_raw', 'sdl_vn_interface_ise_header') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_ise_header__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_interface_ise_header__duplicate_test')}}
        )qualify rnk = 1
),
final as(
    select 
        ise_id::varchar(255) as ise_id,
        ise_desc::varchar(255) as ise_desc,
        channel_code::varchar(255) as channel_code,
        channel_desc::varchar(255) as channel_desc,
        startdate::varchar(255) as startdate,
        enddate::varchar(255) as enddate,
        filename::varchar(255) as filename,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final