{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_promotion') }}
),
final as(
    select *,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
        from source
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
        {% endif %} 
)

select * from final