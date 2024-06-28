{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as (
    select * from {{ source('indsdl_raw', 'sdl_winculum_dailysales')}}
),
final as
 (
    select
    Distcode::varchar(50) as distcode,
	salinvdate::timestamp_ntz(9) as salinvdate,
	salinvno::varchar(50) as salinvno,
	rtrcode::varchar(100) as rtrcode,
	productcode::varchar(50) as productcode,
	prdqty::number(18,0) as prdqty,
	nr::number(18,6) as nr,
	total_price::number(18,6) as total_price,
	tax::number(38,6) as tax,
	filename::varchar(100) as filename,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm    	
    
    from source
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final