{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}


with sdl_ph_pos_puregold as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_puregold') }}
),
final as (
select *
from sdl_ph_pos_puregold
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %})
select * from final