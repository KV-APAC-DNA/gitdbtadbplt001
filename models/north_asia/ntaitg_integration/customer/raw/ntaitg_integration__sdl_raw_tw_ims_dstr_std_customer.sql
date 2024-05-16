{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_customer') }} 
),

final as
(
    select
        *
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
{% endif %} 
)

select * from final