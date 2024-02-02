{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ source('myssdl_raw','sdl_my_dstrbtrr_dim') }} ),
     
final as (
    select * from source
)

select * from final