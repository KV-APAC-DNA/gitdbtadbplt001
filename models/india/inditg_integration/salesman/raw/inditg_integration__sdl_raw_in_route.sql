{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_route') }} 
    where file_name not in (
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_route__null_test')}}
        union all
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_route__duplicate_test')}}
     )
),
final as(
    select 
        cmpcode as cmpcode,
        distcode as distcode,
        distrbrcode as distrbrcode,
        rmcode as rmcode,
        routetype as routetype,
        rmname as rmname,
        distance as distance,
        vanroute as vanroute,
        status as status,
        rmpopulation as rmpopulation,
        localupcountry as localupcountry,
        createddt as createddt,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final