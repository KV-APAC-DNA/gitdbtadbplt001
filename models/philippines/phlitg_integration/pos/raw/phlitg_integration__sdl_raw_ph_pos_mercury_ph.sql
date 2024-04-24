{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}


with sdl_ph_pos_mercury as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_mercury') }}
),
final as (
select *
from sdl_ph_pos_mercury
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %})
select * from final