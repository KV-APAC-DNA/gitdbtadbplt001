{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_sfmc_unsubscribe_data') }}
    where file_name not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_unsubscribe_data__test_null__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_unsubscribe_data__test_duplicate__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_unsubscribe_data__test_lookup__ff')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_sfmc_unsubscribe_data_format_test')}}
    )
),

final as
(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)

select * from final