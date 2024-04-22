{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_account_group as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_group') }}
),
final as (
select
*
from sdl_perenso_account_group
{% if is_incremental() %}
        where sdl_perenso_account_group.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 