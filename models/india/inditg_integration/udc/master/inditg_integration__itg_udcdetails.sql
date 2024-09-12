{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
		pre_hook = "{% if is_incremental() %}
        delete from {{this}} where createddate >= (select min(createddate)
        from {{source('indsdl_raw', 'sdl_csl_udcdetails')}});
        {% endif %}"
    )
}}

with source as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_udcdetails')}}
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        file_name::varchar(225) as file_name
    from source
)
select * from final
