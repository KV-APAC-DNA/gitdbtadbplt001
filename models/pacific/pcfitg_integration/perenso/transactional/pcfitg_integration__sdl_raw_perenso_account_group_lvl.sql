{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_account_group_lvl as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_group_lvl') }}
),
final as (
select
*
from sdl_perenso_account_group_lvl
{% if is_incremental() %}
        where sdl_perenso_account_group_lvl.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 