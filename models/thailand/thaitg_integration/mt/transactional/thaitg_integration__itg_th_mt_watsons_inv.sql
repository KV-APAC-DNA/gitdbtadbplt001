{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons_1209_UAT') }}
),
final as(
    select * from source
    )
select * from final