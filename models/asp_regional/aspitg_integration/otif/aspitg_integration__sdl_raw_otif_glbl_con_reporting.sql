{{
    config(
        materialized='incremental',
        incremental_strategy='append'
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