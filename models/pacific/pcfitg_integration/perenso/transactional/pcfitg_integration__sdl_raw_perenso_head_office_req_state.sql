{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_head_office_req_state as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_head_office_req_state') }}
),
final as (
select
*
from sdl_perenso_head_office_req_state
{% if is_incremental() %}
        where sdl_perenso_head_office_req_state.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 