{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['order_key','batch_key','line_key'],
        pre_hook= "delete from {{this}} where (order_key,batch_key,line_key) in (select distinct order_key,batch_key,line_key from  {{ source('pcfsdl_raw', 'sdl_perenso_order_detail') }});"
    )
}}
with source as 
(
    select *, dense_rank() over(partition by order_key,batch_key,line_key order by run_id desc) as rn from {{ source('pcfsdl_raw', 'sdl_perenso_order_detail') }}
    qualify rn=1
),
final as 
(
    select 
    order_key::number(10,0) as order_key,
	batch_key::number(10,0) as batch_key,
	line_key::number(10,0) as line_key,
	prod_key::number(10,0) as prod_key,
	unit_qty::number(10,0) as unit_qty,
	entered_qty::number(10,0) as entered_qty,
	entered_unit_key::number(10,0) as entered_unit_key,
	trunc(list_price,2)::number(10,2) as list_price,
	trunc(nis,2)::number(10,2) as nis,
	trunc(rrp,2)::number(10,2) as rrp,
	disc_key::number(10,0) as disc_key,
	credit_line_key::number(10,0) as credit_line_key,
	credited::varchar(256) as credited,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final