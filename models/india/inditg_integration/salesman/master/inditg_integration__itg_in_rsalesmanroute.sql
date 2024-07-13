{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where case  when ( select count(*) from {{ source('indsdl_raw', 'sdl_in_salesman_route') }} ) > 0 then 1 else 0 end = 1;
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_salesman_route') }}
),
final as 
(
    select
        cmpcode::varchar(10) as cmpcode,
        distrcode::varchar(50) as distrcode,
        distrbrcode::varchar(30) as distrbrcode,
        salesmancode::varchar(50) as salesmancode,
        routecode::varchar(100) as routecode,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final