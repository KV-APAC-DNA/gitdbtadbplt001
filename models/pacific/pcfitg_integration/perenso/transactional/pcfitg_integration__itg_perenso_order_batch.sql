{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['ord_key','batch_key','branch_key'],
        pre_hook= "delete from {{this}} where (ord_key, batch_key, branch_key) in (select distinct ord_key,batch_key,branch_key from  {{ source('pcfsdl_raw', 'sdl_perenso_order_batch') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_order_batch') }}
),
final as 
(
    select 
    ord_key::number(10,0) as ord_key,
	batch_key::number(10,0) as batch_key,
	branch_key::number(10,0) as branch_key,
	dist_acct::varchar(100) as dist_acct,
	to_date(try_to_timestamp_ntz(delvry_dt,'dd/mm/yyyy')) as delvry_dt,
	status::number(10,0) as status,
	suffix::varchar(10) as suffix,
	to_date(try_to_timestamp_ntz(sent_dt,'dd/mm/yyyy hh12:mi:ss am')) as sent_dt,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final