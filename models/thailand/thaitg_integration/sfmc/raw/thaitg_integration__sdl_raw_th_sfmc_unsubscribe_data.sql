{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_unsubscribe_data') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_unsubscribe_data__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_unsubscribe_data__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_sfmc_unsubscribe_data__lookup_test') }}
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