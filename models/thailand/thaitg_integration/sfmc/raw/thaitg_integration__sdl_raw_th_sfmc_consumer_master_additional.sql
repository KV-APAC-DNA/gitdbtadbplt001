{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }}
),

final as
(
    select
        *
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %} 
)

select * from final