{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_naver_search_channel') }} 
     where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_dl_kr_dads_naver_search_channel__format_test') }})
),
final as (
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final