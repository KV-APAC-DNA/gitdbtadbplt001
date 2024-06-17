
{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}
with sdl_pop6_sg_planned_visits as (
    select * from {{ source('ntasdl_raw', 'sdl_pop6_sg_executed_visits') }}
),
final as (
SELECT *
FROM sdl_pop6_sg_executed_visits
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
{% endif %})
select * from final