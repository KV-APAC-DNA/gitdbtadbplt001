{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_head_office_req_check as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_head_office_req_check') }}
),
final as (
select
*
from sdl_perenso_head_office_req_check
{% if is_incremental() %}
        where sdl_perenso_head_office_req_check.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 