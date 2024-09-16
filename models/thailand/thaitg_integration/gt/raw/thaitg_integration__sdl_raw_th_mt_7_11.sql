{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_7_11') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_7_11__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_7_11__duplicate_test') }}
    )
),
final as(
    select * from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final