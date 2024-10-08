{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{%if is_incremental()%}
        delete from {{this}} where to_date(diary_session_start_time) in (select distinct to_date(try_to_timestamp_ntz(start_time,'dd/mm/yyyy hh12:mi:ss am')) from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item_time') }} 
        where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_perenso_diary_item_time__null_test')}}
        ));{%endif%}"
    )
}}
with source as 
(
    select *, dense_rank() over (partition by to_date(try_to_timestamp_ntz(start_time,'dd/mm/yyyy hh12:mi:ss am')) order by file_name desc) rnk
    from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item_time') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_perenso_diary_item_time__null_test')}}
        ) qualify rnk = 1
),
final as 
(
    select 
    diary_item_key::varchar(50) as diary_item_key,
	to_date(try_to_timestamp_ntz(date_time,'dd/mm/yyyy hh12:mi:ss am')) as date_time,
	to_date(try_to_timestamp_ntz(start_time,'dd/mm/yyyy hh12:mi:ss am')) as diary_session_start_time,
	to_date(try_to_timestamp_ntz(end_time,'dd/mm/yyyy hh12:mi:ss am')) as diary_session_end_time,
	duration::varchar(50) as duration,
	latitude::varchar(50) as latitude,
	longitude::varchar(50) as longitude,
	google_maps_url::varchar(255) as google_maps_url,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt ,
    file_name::varchar(255) as file_name
    from source
)
select * from final