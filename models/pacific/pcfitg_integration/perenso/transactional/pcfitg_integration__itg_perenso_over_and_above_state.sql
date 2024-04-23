{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['store_chk_hdr_key','over_and_above_key'],
        pre_hook= "delete from {{this}} where (store_chk_hdr_key,over_and_above_key) in (select distinct store_chk_hdr_key,over_and_above_key from {{ source('pcfsdl_raw', 'sdl_perenso_over_and_above_state') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_over_and_above_state') }}
),
final as 
(
    select 
    over_and_above_key::number(10,0) as over_and_above_key,
	to_timestamp(to_date(start_date,'dd/mm/yyyy')) as start_date,
	to_timestamp(to_date(end_date,'dd/mm/yyyy')) as end_date,
	batch_count::number(10,0) as batch_count,
	store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final