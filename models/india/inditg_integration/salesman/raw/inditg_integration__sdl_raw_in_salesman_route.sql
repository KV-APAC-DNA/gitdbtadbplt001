{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_salesman_route') }} 
    where file_name not in 
    (select distinct file_name {{ source('indwks_integration', 'TRATBL_sdl_in_salesman_route__null_test') }}
    union all
    select distinct file_name {{ source('indwks_integration', 'TRATBL_sdl_in_salesman_route__duplicate_test') }}
    )
),
final as(
    select 
        cmpcode as cmpcode,
        distrcode as distrcode,
        distrbrcode as distrbrcode,
        salesmancode as salesmancode,
        routecode as routecode,
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