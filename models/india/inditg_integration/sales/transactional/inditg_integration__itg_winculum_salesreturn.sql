{{
    config(
        materialized="incremental",
        incremental_strategy= "append",    
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (filename) in (
        select distinct filename from {{ source('indsdl_raw', 'sdl_winculum_salesreturn') }});
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('indsdl_raw', 'sdl_winculum_salesreturn') }}
),
final as 
(
    select 
    distcode::varchar(50) as distcode,
	srndate::timestamp_ntz(9) as srndate,
	srnrefno::varchar(50) as srnrefno,
	rtrcode::varchar(100) as rtrcode,
	productcode::varchar(50) as productcode,
	prdqty::number(18,0) as prdqty,
	nr::number(18,6) as nr,
	total_price::number(18,6) as total_price,
	tax::number(38,6) as tax,
	filename::varchar(100) as filename,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final