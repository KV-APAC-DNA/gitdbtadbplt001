{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['order_key'],
        pre_hook= "delete from {{this}} where (order_key) in (select distinct order_key from  {{ source('pcfsdl_raw', 'sdl_perenso_order_header') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_order_header') }}
),
final as 
(
    select 
    order_key::number(10,0) as order_key,
	order_type_key::number(10,0) as order_type_key,
	acct_key::number(10,0) as acct_key,
	to_date(try_to_timestamp_ntz(order_date,'dd/mm/yyyy hh12:mi:ss am')) as order_date,
	status::number(10,0) as status,
	charge::varchar(256) as charge,
	confirmation::varchar(256) as confirmation,
	diary_item_key::number(10,0) as diary_item_key,
	work_item_key::number(10,0) as work_item_key,
	account_order_no::varchar(256) as account_order_no,
	delvry_instns::varchar(256) as delvry_instns,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final