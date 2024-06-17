{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with
source as
(
    select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_POP6_TH_TASKS
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