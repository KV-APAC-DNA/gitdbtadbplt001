{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_perenso_todo as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_todo') }}
),
final as (
select
*
from sdl_perenso_todo
{% if is_incremental() %}
        where sdl_perenso_todo.create_dt > (select max(create_dt) from {{ this }}) 
    {% endif%}
)
select * from final 