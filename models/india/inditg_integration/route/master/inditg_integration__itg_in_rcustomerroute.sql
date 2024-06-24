{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where case  when ( select count(*) from {{ source('indsdl_raw', 'sdl_in_retailer_route') }} ) > 0 then 1 else 0 end = 1;
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_retailer_route') }}
),
final as 
(
    select
        cmpcode::varchar(10) as cmpcode,
        distcode::varchar(50) as distcode,
        distrbrcode::varchar(30) as distrbrcode,
        rtrcode::varchar(50) as rtrcode,
        rmcode::varchar(100) as rmcode,
        routetype::varchar(10) as routetype,
        coveragesequence::number(18,0) as coveragesequence,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final