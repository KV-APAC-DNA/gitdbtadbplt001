{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_price_data') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__duplicate_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__null_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_price_data__format_test') }}
    )
),
final as(
    select * from source
    {% if is_incremental() %}
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final