{{
    config(
        materialized="incremental",
        incremental_strategy="append",
       
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_rretailergeoextension') }} 
    where file_name not in (
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_rretailergeoextension__null_test')}}
        union all
        select distinct file_name from {{source('indwks_integration','TRATBL_sdl_in_rretailergeoextension__duplicate_test')}}
)),
final as(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final