{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons') }}
),
final as(
    select *, current_timestamp()::timestamp_ntz(9) as crtd_dttm from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %}
)
select * from final