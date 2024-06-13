{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_promotion_plans') }}
),

final as
(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %}
)

select * from final