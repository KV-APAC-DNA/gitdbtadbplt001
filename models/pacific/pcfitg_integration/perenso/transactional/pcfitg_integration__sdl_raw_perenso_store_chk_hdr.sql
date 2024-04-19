{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_store_chk_hdr as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_store_chk_hdr') }}
),
final as (
select
*
from sdl_perenso_store_chk_hdr
{% if is_incremental() %}
        where sdl_perenso_store_chk_hdr.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 