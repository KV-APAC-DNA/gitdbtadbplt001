{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons') }}
),
final as(
    select * from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where date > (select max(date) from {{ this }}) 
 {% endif %}
)
select * from final