{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    select * from {{ source('myssdl_raw','sdl_my_perfectstore_oos') }}    
),
final as
(
    select * from source
)
select * from final
