{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_retailer_route') }}
    where file_name not in (
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_retailer__null_test')}}
        union all
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_retailer_route__duplicate_test')}}) 
),
final as(
    select 
        cmpcode as cmpcode,
        distcode as distcode,
        distrbrcode as distrbrcode,
        rtrcode as rtrcode,
        rmcode as rmcode,
        routetype as routetype,
        coveragesequence as coveragesequence,
        createddt as createddt,
        filename as filename,
        run_id as run_id,
        crt_dttm as crt_dttm,
        file_name
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final