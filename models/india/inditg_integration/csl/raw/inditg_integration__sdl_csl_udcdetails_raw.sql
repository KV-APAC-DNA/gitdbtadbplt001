{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('indsdl_raw', 'sdl_csl_udcdetails')}}
),
final as
 (
    select
    distcode::varchar(50) as distcode,
	masterid::number(18,0) as masterid,
	mastername::varchar(200) as mastername,
	mastervaluecode::varchar(200) as mastervaluecode,
	mastervaluename::varchar(200) as mastervaluename,
	columnname::varchar(100) as columnname,
	columnvalue::varchar(100) as columnvalue,
	uploadflag::varchar(1) as uploadflag,
	createddate::timestamp_ntz(9) as createddate,
	syncid::number(38,0) as syncid,
	run_id::number(14,0) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm, 
	file_name::varchar(50) as file_name
            
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final
