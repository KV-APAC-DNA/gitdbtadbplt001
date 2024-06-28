{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as
(
    -- select * from {{ source('myssdl_raw','sdl_my_perfectstore_promocomp') }}
    select * from dev_dna_load.snaposesdl_raw.sdl_my_perfectstore_promocomp
    
),
final as
(
    select * from source
)
select * from final