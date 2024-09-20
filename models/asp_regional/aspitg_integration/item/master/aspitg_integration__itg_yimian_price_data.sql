{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="{% if is_incremental() %}
        delete from {{this}} WHERE (file_name) IN (SELECT DISTINCT file_name FROM {{ source('chnsdl_raw', 'sdl_cn_yimian_price_data') }});
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('chnsdl_raw', 'sdl_cn_yimian_price_data') }}
),
final as
(
    select 'CN' as cntry_cd,
    *,
    convert_timezone('UTC',current_timestamp()) as crtd_dttm,
    convert_timezone('UTC',current_timestamp()) as updt_dttm
    FROM source
)
select * from final