
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}
with sdl_pop6_tw_promotion_plans as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_promotion_plans') }}
),
final as (
SELECT *
FROM sdl_pop6_tw_promotion_plans
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %})
select * from final