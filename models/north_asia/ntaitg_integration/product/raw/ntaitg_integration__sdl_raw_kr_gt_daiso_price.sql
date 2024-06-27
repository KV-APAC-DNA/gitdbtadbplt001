{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_gt_daiso_price') }} 
),
final as
(
    select
        *,
        current_timestamp() as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %} 
)
select * from final