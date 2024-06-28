{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
		pre_hook="{% if is_incremental() %}
        delete from {{this}} where createddate >(
        select min(createddate)
        from {{source('indsdl_raw', 'sdl_csl_rdssmweeklytarget_output')}});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_csl_rdssmweeklytarget_output') }}
),
final as 
(
    select
    distcode::varchar(50) as distcode,
	targetrefno::varchar(100) as targetrefno,
	targetdate::timestamp_ntz(9) as targetdate,
	smcode::varchar(50) as smcode,
	smname::varchar(100) as smname,
	rmcode::varchar(50) as rmcode,
	rmname::varchar(100) as rmname,
	targetyear::number(18,0) as targetyear,
	targetmonth::varchar(30) as targetmonth,
	targetvalue::number(36,2) as targetvalue,
	targetname::varchar(50) as targetname,
	week1::number(36,2) as week1,
	week2::number(36,2) as week2,
	week3::number(36,2) as week3,
	week4::number(36,2) as week4,
	week5::number(36,2) as week5,
	targetstatus::varchar(10) as targetstatus,
	targettype::varchar(50) as targettype,
	downloadstatus::varchar(10) as downloadstatus,
	createddate::timestamp_ntz(9) as createddate,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final