{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
		pre_hook="{% if is_incremental() %}
        delete from {{this}} WHERE (cmpcode,rtrhierdfn_code,retlrgroupcode,classcode) 
        IN (SELECT DISTINCT cmpcode, rtrhierdfn_code, retlrgroupcode, classcode
        FROM {{source('indsdl_raw', 'sdl_csl_retailerhierarchy')}});
        {% endif %}"
    )
}}

with source as 
(
    select * from {{source('indsdl_raw', 'sdl_csl_retailerhierarchy')}}
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
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final
