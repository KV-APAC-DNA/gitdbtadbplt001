{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons_pos_1210_UAT') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_SDL_TH_WATSONS_WEEKLY_POS__NULL_TEST') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_SDL_TH_WATSONS_WEEKLY_POS__DUPLICATE_TEST') }}
    )
),
final as(
    select * from source
    {% if is_incremental() %}
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final