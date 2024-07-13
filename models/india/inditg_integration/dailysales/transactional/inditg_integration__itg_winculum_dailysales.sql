{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where filename in (select distinct filename from {{source('indsdl_raw','sdl_winculum_dailysales')}});
        {% endif %}"
    )
}}
with sdl_winculum_dailysales as
(
    select * from {{ source('indsdl_raw', 'sdl_winculum_dailysales') }}
),
final as
(
    select
    distcode::varchar(50) as distcode,
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
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    convert_timezone('Asia/Singapore', CURRENT_TIMESTAMP())::timestamp_ntz(9) as updt_dttm
    from sdl_winculum_dailysales
)
select * from final