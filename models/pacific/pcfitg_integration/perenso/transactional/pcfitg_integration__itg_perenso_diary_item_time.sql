{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['objective_key'],
        pre_hook= "delete from {{this}} WHERE diary_session_start_time IN (SELECT DISTINCT TO_DATE(SUBSTRING(start_time,0,23),'DD/MM/YYYY HH:MI:SS AM')
(select distinct nvl(objective_key, '#') from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item_time') }}"
    )
}}
with source as 
(
    select * from source('pcfsdl_raw', 'sdl_perenso_diary_item_time')
),
final as 
(
    select 
    diary_item_key::varchar(50) as diary_item_key,
	to_date(try_to_timestamp_ntz(date_time,'dd/mm/yyyy hh12:mi:ss am')) as date_time,
	diary_session_start_time::timestamp_ntz(9) as diary_session_start_time,--
	diary_session_end_time::timestamp_ntz(9) as diary_session_end_time,--
	duration::varchar(50) as duration,
	latitude::varchar(50) as latitude,
	longitude::varchar(50) as longitude,
	google_maps_url::varchar(255) as google_maps_url,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final