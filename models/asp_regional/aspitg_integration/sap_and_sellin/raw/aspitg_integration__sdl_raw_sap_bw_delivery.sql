{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ ref('aspitg_integration__vw_stg_sdl_sap_bw_delivery') }}
),
final as(
    select * from source
)

select * from final