{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailerroute_raw')}}
),
final as
(
    select 
        distcode::varchar(100) as distcode,
        rtrid::number(18,0) as rtrid,
        rtrcode::varchar(100) as rtrcode,
        rtrname::varchar(100) as rtrname,
        rmid::number(18,0) as rmid,
        rmcode::varchar(100) as rmcode,
        rmname::varchar(100) as rmname,
        routetype::varchar(100) as routetype,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(50) as file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
