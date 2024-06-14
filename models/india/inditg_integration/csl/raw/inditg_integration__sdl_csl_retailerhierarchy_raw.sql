{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailerhierarchy_raw')}}
),
final as
(
    select 
        cmpcode::varchar(50) as cmpcode,
        rtrhierdfn_code::varchar(25) as rtrhierdfn_code,
        rtrhierdfn_name::varchar(50) as rtrhierdfn_name,
        retlrgroupcode::varchar(50) as retlrgroupcode,
        retlrgroupname::varchar(50) as retlrgroupname,
        classcode::varchar(10) as classcode,
        classdesc::varchar(100) as classdesc,
        turnover::number(18,4) as turnover,
        createddt::timestamp_ntz(9) as createddt,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        file_name::varchar(50) as file_name
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %})
select * from final
