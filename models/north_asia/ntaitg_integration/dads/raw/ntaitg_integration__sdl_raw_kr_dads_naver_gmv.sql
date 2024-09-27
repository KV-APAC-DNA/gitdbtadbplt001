{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_naver_gmv') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_dads_naver_gmv__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_dads_naver_gmv__format_test') }}
     )
),
final as (
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final