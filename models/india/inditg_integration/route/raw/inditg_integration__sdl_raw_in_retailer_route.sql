{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_retailer_route') }} 
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
        crt_dttm as crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final