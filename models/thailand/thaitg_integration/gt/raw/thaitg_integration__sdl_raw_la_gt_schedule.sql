{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_la_gt_schedule') }}
),
final as(
    select * from source
    {% if is_incremental() %}
        where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final