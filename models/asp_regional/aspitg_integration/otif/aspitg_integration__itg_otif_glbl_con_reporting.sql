--{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where rdd_exp >= (select distinct min(rdd_exp) from {{ source('aspsdl_raw', 'sdl_otif_glbl_con_reporting') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_otif_glbl_con_reporting') }}
),
final as
(
    select * from source
)
select * from final