{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with sdl_ph_iop_trgt as (
select * from {{ source('phlsdl_raw', 'sdl_ph_iop_trgt') }}
),
final as (
select *
from sdl_ph_iop_trgt 
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
 )
select * from final 